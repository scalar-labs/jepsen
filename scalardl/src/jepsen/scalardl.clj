(ns jepsen.scalardl
  (:require [cassandra.core :as cassandra]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [cli :as cli]
             [client :as client]
             [checker :as checker]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [independent :as independent]
             [nemesis :as nemesis]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.scalardl.support :as s]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [knossos.op])
  (:import (com.scalar.client.service ClientService)
           (java.util Optional)
           (com.scalar.rpc ContractExecutionResponse)
           (com.scalar.ledger.service StatusCode)))

(defn parse-long
  [s]
  (when s (Long/parseLong s)))

(def dir "/opt/scalardl")
(def binary "bin/scalar-ledger")
(def logfile (str dir "/scalardl.log"))
(def pidfile (str dir "/scalardl.pid"))
(def ledger-dir "/jepsen/scalardl/ledger")

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defrecord Client [conn]
  client/Client
  (setup! [this test]
    (when @(:register-contracts test)
      (reset! (:register-contracts test) false)

      (info "registering certificates and contracts")
      (.registerCertificate conn)
      (.registerContract
        conn
        "read"
        "com.scalar.jepsen.scalardl.Read"
        (str ledger-dir "/Read.class")
        (Optional/empty))
      (.registerContract
        conn
        "write"
        "com.scalar.jepsen.scalardl.Write"
        (str ledger-dir "/Write.class")
        (Optional/empty))))

  (open! [this test node]
    (let [clientService (.getInstance (s/create-injector node) ClientService)]
      (assoc this :conn clientService)))

  (close! [this test]
    (.close conn))

  (invoke! [_ test op]
    (let [[k v] (:value op)]
      (case (:f op)
        :read (let [argument (s/create-argument k)
                    response (.executeContract conn "read" argument)]
                (if (= (.get StatusCode/OK) (.getStatus response))
                  (assoc op :type :ok, :value (independent/tuple k (s/response->int response)))
                  (assoc op :type :fail, :error (.getMessage response))))
        :write (let [argument (s/create-argument k v)
                     response (.executeContract conn "write" argument)]
                 (if (= (.get StatusCode/OK) (.getStatus response))
                   (assoc op :type :ok)
                   (assoc op :type :fail, :error (.getMessage response)))))))

  (teardown! [_ test]))

(defn db
  [version]
  (reify db/DB
    (setup! [_ test node]
      ; we don't always wipe Cassandra after a test, so at least do it before
      (cassandra/wipe! node)
      (s/spinup-cassandra! test node)

      (c/upload [(str ledger-dir "/ledger.tar")
                 (str ledger-dir "/ledger.properties")
                 (str ledger-dir "/client.pem")
                 (str ledger-dir "/client-key.pem")]
                "/tmp")

      (when (= node (first (:nodes test)))
           (info node "creating schema for scalardl")
           (s/create-schema! (vector node) (:rf test)))

      (info node "installing scalardl")
      (cu/install-archive! "file:///tmp/ledger.tar" dir)
      (c/exec (c/lit (str "sed -i -e s/scalar.database.contact_points=.*/scalar.database.contact_points="
                          node
                          "/ /tmp/ledger.properties")))

      (cu/start-daemon!
        {:logfile logfile
         :pidfile pidfile
         :chdir   dir}
        binary
        :-config "/tmp/ledger.properties")
      (Thread/sleep 2000))  ; sleeping is necessary?

    (teardown! [_ test node]
      (info node "tearing down scalardl")
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir))
      (if (:keep-cassandra test)
        (cassandra/stop! node)
        (cassandra/wipe! node)))

    db/LogFiles
    (log-files [_ test node]
      [logfile
       "/root/cassandra/logs/system.log"])))

(def cli-opts
  [[nil "--keep-cassandra" "Do not wipe Cassandra after the test is over."]
   [nil "--rf REPLICATION_FACTOR" "Replication factor for Cassandra."
    :default 3
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default 10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number."]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default 100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]
   (cli/tarball-opt "https://archive.apache.org/dist/cassandra/3.11.4/apache-cassandra-3.11.4-bin.tar.gz")])

(defn scalardl-test
  [opts]
  (merge tests/noop-test
         opts
         {:name           "scalardl"
          :os             debian/os
          :db             (db "3.11.4")
          :register-contracts (atom true)
          :decommissioned (atom #{})    ; needed to avoid NullPointerExceptions in Cassandra tests
          :client         (Client. nil)
          :nemesis        (nemesis/partition-random-halves)
          :checker        (independent/checker
                            (checker/linearizable {:model     (model/register)
                                                   :algorithm :linear}))
          :generator      (->> (independent/concurrent-generator
                                 1      ; threads per key
                                 (range)
                                 (fn [k]
                                   (->> (gen/mix [r w])
                                        (gen/stagger (/ (:rate opts)))
                                        (gen/limit (:ops-per-key opts)))))
                               (gen/nemesis
                                 (gen/seq (cycle [(gen/sleep 5)
                                                  {:type :info, :f :start}
                                                  (gen/sleep 5)
                                                  {:type :info, :f :stop}])))
                               (gen/time-limit (:time-limit opts)))}))

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn scalardl-test
                                  :opt-spec cli-opts})
            args))

