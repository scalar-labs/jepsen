(ns cassandra.core
  (:require [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.java.io :as io]
            [clojure.java.jmx :as jmx]
            [clojure.set :as set]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
             [db        :as db]
             [util      :as util :refer [meh timeout]]
             [control   :as c :refer [| lit]]
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]
             [store     :as store]
             [report    :as report]
             [tests     :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control [net :as net]
             [util :as net/util]]
            [jepsen.os.debian :as debian]
            [knossos.core :as knossos]
            [clojurewerkz.cassaforte.metadata :as metadata]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clojurewerkz.cassaforte.query :refer :all]
            [clojurewerkz.cassaforte.policies :refer :all]
            [clojurewerkz.cassaforte.cql :as cql])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core ConsistencyLevel)
           (com.datastax.driver.core.schemabuilder SchemaBuilder)
           (com.datastax.driver.core.policies RetryPolicy
                                              RetryPolicy$RetryDecision)
           (java.net InetAddress)))

(defn scaled
  "Applies a scaling factor to a number - used for durations
  throughout testing to easily scale the run time of the whole
  test suite. Accepts doubles."
  [v]
  (let [factor (or (some-> (System/getenv "JEPSEN_SCALE") (Double/parseDouble))
                   1)]
    (Math/ceil (* v factor))))

(defn compaction-strategy
  "Returns the compaction strategy to use"
  []
  (SchemaBuilder/sizedTieredStategy))

(defn compressed-commitlog?
  "Returns whether to use commitlog compression"
  []
  (= (some-> (System/getenv "JEPSEN_COMMITLOG_COMPRESSION") (clojure.string/lower-case))
     "true"))

(defn coordinator-batchlog-disabled?
  "Returns whether to disable the coordinator batchlog for MV"
  []
  (boolean (System/getenv "JEPSEN_DISABLE_COORDINATOR_BATCHLOG")))

(defn phi-level
  "Returns the value to use for phi in the failure detector"
  []
  (or (System/getenv "JEPSEN_PHI_VALUE")
      8))

(defn disable-hints?
  "Returns true if Jepsen tests should run without hints"
  []
  (not (System/getenv "JEPSEN_DISABLE_HINTS")))

(defn wait-for-recovery
  "Waits for the driver to report all nodes are up"
  [timeout-secs conn]
  (timeout (* 1000 timeout-secs)
           (throw (RuntimeException.
                   (str "Driver didn't report all nodes were up in "
                        timeout-secs "s - failing")))
           (while (->> (metadata/hosts conn)
                       (map :is-up) and not)
             (Thread/sleep 500))))

(defn dns-resolve
  "Gets the address of a hostname"
  [hostname]
  (.getHostAddress (InetAddress/getByName (name hostname))))

(defn dns-hostnames
  "Gets the list of hostnames"
  [test addrs]
  (let [names (:nodes test)
        ordered (map dns-resolve names)]
    (set (map (fn [addr]
                (->> (.indexOf ordered addr)
                     (get names)))
               addrs))))

(defn live-nodes
  "Get the list of live nodes from a random node in the cluster"
  [test]
  (->> (some (fn [node]
               (try (jmx/with-connection {:host (name node) :port 7199}
                      (jmx/read "org.apache.cassandra.db:type=StorageService"
                                :LiveNodes))
                      (catch Exception e
                        (info "Couldn't get status from node" node))))
               (-> test :nodes set (set/difference @(:bootstrap test))
                   set (set/difference @(:decommission test))
                   shuffle))
    (dns-hostnames test)))

(defn joining-nodes
  "Get the list of joining nodes from a random node in the cluster"
  [test]
  (set (mapcat (fn [node]
                 (try (jmx/with-connection {:host (name node) :port 7199}
                        (jmx/read "org.apache.cassandra.db:type=StorageService"
                                  :JoiningNodes))
                      (catch Exception e
                        (info "Couldn't get status from node" node))))
               (-> test :nodes set (set/difference @(:bootstrap test))
                   (#(map (comp dns-resolve name) %)) set (set/difference @(:decommission test))
                   shuffle))))

(defn nodetool
  "Run a nodetool command"
  [node & args]
  (c/on node (apply c/exec (lit "~/cassandra/bin/nodetool") args)))

; This policy should only be used for final reads! It tries to
; aggressively get an answer from an unstable cluster after
; stabilization
(def aggressive-read
  (proxy [RetryPolicy] []
    (onReadTimeout [statement cl requiredResponses
                    receivedResponses dataRetrieved nbRetry]
      (if (> nbRetry 100)
        (RetryPolicy$RetryDecision/rethrow)
        (RetryPolicy$RetryDecision/retry cl)))
    (onWriteTimeout [statement cl writeType requiredAcks
                     receivedAcks nbRetry]
      (RetryPolicy$RetryDecision/rethrow))
    (onUnavailable [statement cl requiredReplica aliveReplica nbRetry]
      (info "Caught UnavailableException in driver - sleeping 2s")
      (Thread/sleep 2000)
      (if (> nbRetry 100)
        (RetryPolicy$RetryDecision/rethrow)
        (RetryPolicy$RetryDecision/retry cl)))))

(def setup-lock (Object.))

(defn cached-install?
  [src]
  (try (c/exec :grep :-s :-F :-x (lit src) (lit ".download"))
       true
       (catch RuntimeException _ false)))

(defn install!
  "Installs Cassandra on the given node."
  [node version]
  (c/su
   (c/cd
    "/tmp"
    (let [tpath (System/getenv "CASSANDRA_TARBALL_PATH")
          url (or tpath
                  (System/getenv "CASSANDRA_TARBALL_URL")
                  (str "http://www.us.apache.org/dist/cassandra/" version
                       "/apache-cassandra-" version "-bin.tar.gz"))]
      (info node "installing Cassandra from" url)
      (if (cached-install? url)
        (info "Used cached install on node" node)
        (do (if tpath
              (c/upload tpath "/tmp/cassandra.tar.gz")
              (c/exec :wget :-O "cassandra.tar.gz" url (lit ";")))
            (c/exec :tar :xzvf "cassandra.tar.gz" :-C "~")
            (c/exec :rm :-r :-f (lit "~/cassandra"))
            (c/exec :mv (lit "~/apache* ~/cassandra"))
            (c/exec :echo url :> (lit ".download"))
            (c/exec
             :echo
             "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main"
             :>"/etc/apt/sources.list.d/webupd8team-java.list")
            (c/exec
             :echo
             "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main"
             :>> "/etc/apt/sources.list.d/webupd8team-java.list")
            (try (c/exec :apt-key :adv :--keyserver "hkp://keyserver.ubuntu.com:80"
                        :--recv-keys "EEA14886")
                 (debian/update!)
                 (catch RuntimeException e
                   (info "Error updating caused by" e)))
            (c/exec :echo
                    "debconf shared/accepted-oracle-license-v1-1 select true"
                    | :debconf-set-selections)
            (debian/install [:oracle-java8-installer])))))))

(defn configure!
  "Uploads configuration files to the given node."
  [node test]
  (info node "configuring Cassandra")
  (c/su
   (doseq [rep ["\"s/#MAX_HEAP_SIZE=.*/MAX_HEAP_SIZE='512M'/g\""
                "\"s/#HEAP_NEWSIZE=.*/HEAP_NEWSIZE='128M'/g\""
                "\"s/LOCAL_JMX=yes/LOCAL_JMX=no/g\""
                (str "'s/# JVM_OPTS=\"$JVM_OPTS -Djava.rmi.server.hostname="
                     "<public name>\"/JVM_OPTS=\"$JVM_OPTS -Djava.rmi.server.hostname="
                     (name node) "\"/g'"
                     )
                (str "'s/JVM_OPTS=\"$JVM_OPTS -Dcom.sun.management.jmxremote"
                     ".authenticate=true\"/JVM_OPTS=\"$JVM_OPTS -Dcom.sun.management"
                     ".jmxremote.authenticate=false\"/g'")
                "'/JVM_OPTS=\"$JVM_OPTS -Dcassandra.mv_disable_coordinator_batchlog=.*\"/d'"]]
     (c/exec :sed :-i (lit rep) "~/cassandra/conf/cassandra-env.sh"))
   (doseq [rep (into ["\"s/cluster_name: .*/cluster_name: 'jepsen'/g\""
                      "\"s/row_cache_size_in_mb: .*/row_cache_size_in_mb: 20/g\""
                      "\"s/seeds: .*/seeds: 'n1,n2'/g\""
                      (str "\"s/listen_address: .*/listen_address: " (dns-resolve node)
                           "/g\"")
                      (str "\"s/rpc_address: .*/rpc_address: " (dns-resolve node) "/g\"")
                      (str "\"s/broadcast_rpc_address: .*/broadcast_rpc_address: "
                           (net/local-ip) "/g\"")
                      "\"s/internode_compression: .*/internode_compression: none/g\""
                      (str "\"s/hinted_handoff_enabled:.*/hinted_handoff_enabled: "
                           (disable-hints?) "/g\"")
                      "\"s/commitlog_sync: .*/commitlog_sync: batch/g\""
                      (str "\"s/# commitlog_sync_batch_window_in_ms: .*/"
                           "commitlog_sync_batch_window_in_ms: 1.0/g\"")
                      "\"s/commitlog_sync_period_in_ms: .*/#/g\""
                      (str "\"s/# phi_convict_threshold: .*/phi_convict_threshold: " (phi-level)
                           "/g\"")
                      "\"/auto_bootstrap: .*/d\""]
                     (when (compressed-commitlog?)
                       ["\"s/#commitlog_compression.*/commitlog_compression:/g\""
                        (str "\"s/#   - class_name: LZ4Compressor/"
                             "    - class_name: LZ4Compressor/g\"")]))]
     (c/exec :sed :-i (lit rep) "~/cassandra/conf/cassandra.yaml"))
   (c/exec :echo (str "JVM_OPTS=\"$JVM_OPTS -Dcassandra.mv_disable_coordinator_batchlog="
                      (coordinator-batchlog-disabled?) "\"")
           :>> "~/cassandra/conf/cassandra-env.sh")
   (c/exec :sed :-i (lit "\"s/INFO/DEBUG/g\"") "~/cassandra/conf/logback.xml")
   (c/exec :echo (str "auto_bootstrap: " (boolean ((-> test :bootstrap deref) node)))
           :>> "~/cassandra/conf/cassandra.yaml")))

(defn start!
  "Starts Cassandra."
  [node test]
  (info node "starting Cassandra")
  (c/su
   (c/exec (lit "~/cassandra/bin/cassandra -R"))
   (c/exec :sleep :60)))

(defn guarded-start!
  "Guarded start that only starts nodes that have joined the cluster already
  through initial DB lifecycle or a bootstrap. It will not start decommissioned
  nodes."
  [node test]
  (let [bootstrap (:bootstrap test)
        decommission (:decommission test)]
    (when-not (or (@bootstrap node) (->> node name dns-resolve (get decommission)))
      (start! node test))))

(defn stop!
  "Stops Cassandra."
  [node]
  (info node "stopping Cassandra")
  (c/su
   (meh (c/exec :killall :java))
   (while (.contains (c/exec :ps :-ef) "java")
     (Thread/sleep 100)))
  (info node "has stopped Cassandra"))

(defn wipe!
  "Shuts down Cassandra and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su
   (meh (c/exec :rm :-r "~/cassandra/logs"))
   (meh (c/exec :rm :-r "~/cassandra/data/data"))
   (meh (c/exec :rm :-r "~/cassandra/data/hints"))
   (meh (c/exec :rm :-r "~/cassandra/data/commitlog"))
   (meh (c/exec :rm :-r "~/cassandra/data/saved_caches"))))

(defn db
  "Cassandra for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (when (seq (System/getenv "LEAVE_CLUSTER_RUNNING"))
        (wipe! node))
      (doto node
        (install! version)
        (configure! test)
        (guarded-start! test)))

    (teardown! [_ test node]
      (when-not (seq (System/getenv "LEAVE_CLUSTER_RUNNING"))
          (wipe! node)))

    db/LogFiles
    (log-files [db test node]
      ["~/cassandra/logs/system.log"])))

(def add {:type :invoke :f :add :value 1})
(def sub {:type :invoke :f :add :value -1})
(def r {:type :invoke :f :read})
(defn w [_ _] {:type :invoke :f :write :value (rand-int 5)})
(defn cas [_ _] {:type :invoke :f :cas :value [(rand-int 5) (rand-int 5)]})

(defn adds
  "Generator that emits :add operations for sequential integers."
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       gen/seq))

(defn assocs
  "Generator that emits :assoc operations for sequential integers,
  mapping x to (f x)"
  [f]
  (->> (range)
       (map (fn [x] {:type :invoke :f :assoc :value {:k x
                                                     :v (f x)}}))
       gen/seq))

(defn read-once
  "A generator which reads exactly once."
  []
  (gen/clients
   (gen/once r)))

(defn cassandra-test
  [name opts]
  (merge tests/noop-test
         {:name    (str "cassandra-" name)
          :os      debian/os}
         opts))
