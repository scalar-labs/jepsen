(ns cassandra.batch
  (:require [clojure [pprint :refer :all]
             [string :as str]
             [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
             [codec     :as codec]
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

(defrecord BatchSetClient [conn]
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
        (cql/create-table conn "bat"
                          (if-not-exists)
                          (column-definitions {:pid :int
                                               :cid :int
                                               :value :int
                                               :primary-key [:pid :cid]}))
        ; @TODO change compaction storategy
        (cql/alter-table conn "bat"
                          (with {:compaction-options (compaction-strategy)}))
        (->BatchSetClient conn))))
  (invoke! [this test op]
    (case (:f op)
      :add (try (let [value (:value op)]
                  (cassandra/execute
                    conn
                    (str "BEGIN BATCH "
                         "INSERT INTO bat (pid, cid, value) VALUES ("
                         value ", 0," value "); "
                         "INSERT INTO bat (pid, cid, value) VALUES ("
                         value ", 1," value "); "
                         "APPLY BATCH;")
                    :consistency-level (consistency-level :quorum)))
                (assoc op :type :ok)
                (catch UnavailableException e
                  (assoc op :type :fail :value (.getMessage e)))
                (catch WriteTimeoutException e
                  (assoc op :type :info :value :timed-out))
                (catch NoHostAvailableException e
                  (info "All nodes are down - sleeping 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :value (.getMessage e))))
      :read (try (let [results (cassandra/execute
                                 conn
                                 "SELECT * from bat"
                                 :consistency-level (consistency-level :all)
                                 :retry-policy aggressive-read)
                       value-a (->> results
                                    (filter (fn [r] (= (:cid r) 0)))
                                    (map :value)
                                    (into (sorted-set)))
                       value-b (->> results
                                    (filter (fn [r] (= (:cid r) 1)))
                                    (map :value)
                                    (into (sorted-set)))]
                   (if-not (= value-a value-b)
                     (assoc op :type :fail :value [value-a value-b])
                     (assoc op :type :ok :value value-a)))
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

(defn batch-set-client
  "A set implemented using batched inserts"
  []
  (->BatchSetClient nil))

(defn batch-set-test
  [name opts]
  (merge (cassandra-test (str "batch set " name)
                         {:client (batch-set-client)
                          :model (model/set)
                          :generator (gen/phases
                                      (->> [(adds)]
                                           (conductors/std-gen opts 60))
                                      (gen/delay 5
                                                 (read-once)))
                          :checker (checker/compose
                                    {:set (checker/set)})})
         (conductors/combine-nemesis opts)))

(def bridge-test
  (batch-set-test "bridge"
                  {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-test
  (batch-set-test "halves"
                  {:nemesis (nemesis/partition-random-halves)}))

(def isolate-node-test
  (batch-set-test "isolate node"
                  {:nemesis (nemesis/partition-random-node)}))

(def crash-subset-test
  (batch-set-test "crash"
                  {:nemesis (crash-nemesis)}))

(def clock-drift-test
  (batch-set-test "clock drift"
                  {:nemesis (nemesis/clock-scrambler 10000)}))

(def flush-compact-test
  (batch-set-test "flush and compact"
                  {:nemesis (conductors/flush-and-compacter)}))

(def bridge-test-bootstrap
  (batch-set-test "bridge bootstrap"
                  {:bootstrap (atom #{"n4" "n5"})
                   :nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-test-bootstrap
  (batch-set-test "halves bootstrap"
                  {:bootstrap (atom #{"n4" "n5"})
                   :nemesis (nemesis/partition-random-halves)}))

(def isolate-node-test-bootstrap
  (batch-set-test "isolate node bootstrap"
                  {:bootstrap (atom #{"n4" "n5"})
                   :nemesis (nemesis/partition-random-node)}))

(def crash-subset-test-bootstrap
  (batch-set-test "crash bootstrap"
                  {:bootstrap (atom #{"n4" "n5"})
                   :nemesis (crash-nemesis)}))

(def clock-drift-test-bootstrap
  (batch-set-test "clock drift bootstrap"
                  {:bootstrap (atom #{"n4" "n5"})
                   :nemesis (nemesis/clock-scrambler 10000)}))

(def bridge-test-decommission
  (batch-set-test "bridge decommission"
                  {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                   :decommissioner true}))

(def halves-test-decommission
  (batch-set-test "halves decommission"
                  {:nemesis (nemesis/partition-random-halves)
                   :decommissioner true}))

(def isolate-node-test-decommission
  (batch-set-test "isolate node decommission"
                  {:nemesis (nemesis/partition-random-node)
                   :decommissioner true}))

(def crash-subset-test-decommission
  (batch-set-test "crash decommission"
                  {:nemesis (crash-nemesis)
                   :decommissioner true}))

(def clock-drift-test-decommission
  (batch-set-test "clock drift decommission"
                  {:nemesis (nemesis/clock-scrambler 10000)
                   :decommissioner true}))

(def bridge-test-mix
  (batch-set-test "bridge bootstrap and decommission"
                  {:bootstrap (atom #{"n5"})
                   :nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                   :decommissioner true}))

(def halves-test-mix
  (batch-set-test "halves bootstrap and decommission"
                  {:bootstrap (atom #{"n5"})
                   :nemesis (nemesis/partition-random-halves)
                   :decommissioner true}))

(def isolate-node-test-mix
  (batch-set-test "isolate node  bootstrap and decommission"
                  {:bootstrap (atom #{"n5"})
                   :nemesis (nemesis/partition-random-node)
                   :decommissioner true}))

(def crash-subset-test-mix
  (batch-set-test "crash bootstrap and decommission"
                  {:bootstrap (atom #{"n5"})
                   :nemesis (crash-nemesis)
                   :decommissioner true}))

(def clock-drift-test-mix
  (batch-set-test "clock drift bootstrap and decommission"
                  {:bootstrap (atom #{"n5"})
                   :nemesis (nemesis/clock-scrambler 10000)
                   :decommissioner true}))
