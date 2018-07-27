(ns cassandra.collections.set
  (:require [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.java.io :as io]
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
            [knossos.model :as model]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clojurewerkz.cassaforte.query :refer :all]
            [clojurewerkz.cassaforte.policies :refer :all]
            [clojurewerkz.cassaforte.cql :as cql]
            [cassandra.core :refer :all]
            [cassandra.conductors :as conductors])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core ConsistencyLevel)
           (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(defrecord CQLSetClient [conn writec]
  client/Client
  (setup! [_ test node]
    (locking setup-lock
      (let [conn (cassandra/connect (->> test :nodes (map name)))]
        (cql/create-keyspace conn "jepsen_keyspace"
                             (if-not-exists)
                             (with {:replication
                                    {"class" "SimpleStrategy"
                                     "replication_factor" 3}}))
        (cql/use-keyspace conn "jepsen_keyspace")
        (cql/create-table conn "sets"
                          (if-not-exists)
                          (column-definitions {:id :int
                                               :elements (set-type :int)
                                               :primary-key [:id]}))
        ; @TODO change compaction storategy
        (cql/alter-table conn "sets"
                          (with {:compaction-options (compaction-strategy)}))
        (cql/insert conn "sets"
                    {:id 0
                     :elements #{}}
                    (if-not-exists))
        (->CQLSetClient conn writec))))
  (invoke! [this test op]
    (case (:f op)
      :add (try (cassandra/execute
                  conn
                  (str "UPDATE sets SET elements = elements + {"
                       (:value op)
                       "} WHERE id = 0;")
                  :consistency-level (consistency-level writec))
                (assoc op :type :ok)
                (catch UnavailableException e
                  (assoc op :type :fail :value (.getMessage e)))
                (catch WriteTimeoutException e
                  (assoc op :type :info :value :timed-out))
                (catch NoHostAvailableException e
                  (info "All nodes are down - sleeping 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :value (.getMessage e))))
      :read (try (wait-for-recovery 30 conn)
                 (let [value (->> (cassandra/execute
                                    conn
                                    "SELECT * from sets WHERE id = 0;"
                                    :consistency-level (consistency-level :all)
                                    :retry-policy aggressive-read)
                                  first
                                  :elements
                                  (into (sorted-set)))]
                   (assoc op :type :ok :value value))
                 (catch UnavailableException e
                   (info "Not enough replicas - failing")
                   (assoc op :type :fail :value (.getMessage e)))
                 (catch ReadTimeoutException e
                   (assoc op :type :fail :value :timed-out))
                 (catch NoHostAvailableException e
                   (info "All nodes are down - sleeping 2s")
                   (Thread/sleep 2000)
                   (assoc op :type :fail :value (.getMessage e))))))
  (teardown! [_ _]
    (info "Tearing down client with conn" conn)
    (cassandra/disconnect! conn)))

(defn cql-set-client
  "A set implemented using CQL sets"
  ([] (->CQLSetClient nil ConsistencyLevel/ONE))
  ([writec] (->CQLSetClient nil writec)))

(defn cql-set-test
  [name opts]
  (merge (cassandra-test (str "cql set " name)
                         {:client (cql-set-client)
                          :model (model/set)
                          :generator (gen/phases
                                      (->> [(adds)]
                                           (conductors/std-gen opts 60))
                                      (read-once))
                          :checker (checker/compose
                                    {:set (checker/set)})})
         (conductors/combine-nemesis opts)))

(def bridge-test
  (cql-set-test "bridge"
                {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-test
  (cql-set-test "halves"
                {:nemesis (nemesis/partition-random-halves)}))

(def isolate-node-test
  (cql-set-test "isolate node"
                {:nemesis (nemesis/partition-random-node)}))

(def crash-subset-test
  (cql-set-test "crash"
                {:nemesis (crash-nemesis)}))

(def flush-compact-test
  (cql-set-test "flush and compact"
                {:nemesis (conductors/flush-and-compacter)}))

(def bridge-test-bootstrap
  (cql-set-test "bridge bootstrap"
                {:bootstrap (atom #{"n4" "n5"})
                 :nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-test-bootstrap
  (cql-set-test "halves bootstrap"
                {:bootstrap (atom #{"n4" "n5"})
                 :nemesis (nemesis/partition-random-halves)}))

(def isolate-node-test-bootstrap
  (cql-set-test "isolate node bootstrap"
                {:bootstrap (atom #{"n4" "n5"})
                 :nemesis (nemesis/partition-random-node)}))

(def crash-subset-test-bootstrap
  (cql-set-test "crash bootstrap"
                {:bootstrap (atom #{"n4" "n5"})
                 :nemesis (crash-nemesis)}))

(def bridge-test-decommission
  (cql-set-test "bridge decommission"
                {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                 :decommissioner true}))

(def halves-test-decommission
  (cql-set-test "halves decommission"
                {:nemesis (nemesis/partition-random-halves)
                 :decommissioner true}))

(def isolate-node-test-decommission
  (cql-set-test "isolate node decommission"
                {:nemesis (nemesis/partition-random-node)
                 :decommissioner true}))

(def crash-subset-test-decommission
  (cql-set-test "crash decommission"
                {:client (cql-set-client :quorum)
                 :nemesis (crash-nemesis)
                 :decommissioner true}))

(def bridge-test-mix
  (cql-set-test "bridge bootstrap and decommission"
                     {:bootstrap (atom #{"n5"})
                      :nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                      :decommissioner true}))

(def halves-test-mix
  (cql-set-test "halves bootstrap and decommission"
                     {:bootstrap (atom #{"n5"})
                      :nemesis (nemesis/partition-random-halves)
                      :decommissioner true}))

(def isolate-node-test-mix
  (cql-set-test "isolate node bootstrap and decommission"
                     {:bootstrap (atom #{"n5"})
                      :nemesis (nemesis/partition-random-node)
                      :decommissioner true}))

(def crash-subset-test-mix
  (cql-set-test "crash bootstrap and decommission"
                     {:bootstrap (atom #{"n5"})
                      :nemesis (crash-nemesis)
                      :decommissioner true}))
