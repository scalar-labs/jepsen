(ns cassandra.counter
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
                                                NoHostAvailableException)
           (com.datastax.driver.core.policies FallthroughRetryPolicy)))

(defrecord CQLCounterClient [conn writec]
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
        (cql/create-table conn "counters"
                          (if-not-exists)
                          (column-definitions {:id :int
                                               :count :counter
                                               :primary-key [:id]}))
        ; @TODO change compaction storategy
        (cql/alter-table conn "counters"
                          (with {:compaction-options (compaction-strategy)}))
        (cql/update conn "counters" {:count (increment-by 0)}
                    (where [[= :id 0]]))
        (->CQLCounterClient conn writec))))
  (invoke! [this test op]
    (case (:f op)
      :add (try (let [value (:value op)
                      added (if (pos? value) (str "+" value) (str value))]
                  (cassandra/execute
                    conn
                    (str "UPDATE counters SET count = "
                         "count " added " WHERE id = 0;")
                    :consistency-level (consistency-level writec)
                    :retry-policy FallthroughRetryPolicy/INSTANCE))
                  (assoc op :type :ok)
                (catch UnavailableException e
                  (assoc op :type :fail :error (.getMessage e)))
                (catch WriteTimeoutException e
                  (assoc op :type :fail :error :timed-out))
                (catch NoHostAvailableException e
                  (info "All the servers are down - waiting 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :error (.getMessage e))))
      :read (try (let [value (->> (cassandra/execute
                                    conn
                                    "SELECT * from counters WHERE id = 0;"
                                    :consistency-level (consistency-level :all)
                                    :retry-policy FallthroughRetryPolicy/INSTANCE)
                                  first
                                  :count)]
                   (assoc op :type :ok :value value))
                 (catch UnavailableException e
                   (info "Not enough replicas - failing")
                   (assoc op :type :fail :error (.getMessage e)))
                 (catch ReadTimeoutException e
                   (assoc op :type :fail :error :timed-out))
                 (catch NoHostAvailableException e
                   (info "All the servers are down - waiting 2s")
                   (Thread/sleep 2000)
                   (assoc op :type :fail :error (.getMessage e))))))
  (teardown! [_ _]
    (info "Tearing down client with conn" conn)
    (cassandra/disconnect! conn)))

(defn cql-counter-client
  "A counter implemented using CQL counters"
  ([] (->CQLCounterClient nil :one))
  ([writec] (->CQLCounterClient nil writec)))

(defn cql-counter-inc-test
  [name opts]
  (merge (cassandra-test (str "cql counter inc " name)
                         {:client (cql-counter-client)
                          :model nil
                          :generator (->> (repeat 100 add)
                                          (cons r)
                                          (conductors/std-gen opts 60))
                          :checker (checker/compose
                                    {:counter (checker/counter)})})
         (conductors/combine-nemesis opts)))

;; TODO: check sub operations
;(defn cql-counter-inc-dec-test
;  [name opts]
;  (merge (cassandra-test (str "cql counter inc dec " name)
;                         {:client (cql-counter-client)
;                          :model nil
;                          :generator (->> (take 100 (cycle [add sub]))
;                                          (cons r)
;                                          (conductors/std-gen opts 60))
;                          :checker (checker/compose
;                                    {:counter (checker/counter)})})
;         (conductors/combine-nemesis opts)))

(def bridge-inc-test
  (cql-counter-inc-test "bridge"
                        {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-inc-test
  (cql-counter-inc-test "halves"
                        {:nemesis (nemesis/partition-random-halves)}))

(def isolate-node-inc-test
  (cql-counter-inc-test "isolate node"
                        {:nemesis (nemesis/partition-random-node)}))

(def flush-compact-inc-test
  (cql-counter-inc-test "flush and compact"
                        {:nemesis (conductors/flush-and-compacter)}))

(def crash-subset-inc-test
  (cql-counter-inc-test "crash"
                        {:nemesis (crash-nemesis)}))

;(def bridge-inc-dec-test
;  (cql-counter-inc-dec-test "bridge"
;                            {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))
;
;(def halves-inc-dec-test
;  (cql-counter-inc-dec-test "halves"
;                            {:nemesis (nemesis/partition-random-halves)}))
;
;(def isolate-node-inc-dec-test
;  (cql-counter-inc-dec-test "isolate node"
;                            {:nemesis (nemesis/partition-random-node)}))
;
;(def crash-subset-inc-dec-test
;  (cql-counter-inc-dec-test "crash"
;                            {:nemesis (crash-nemesis)}))
;
;(def flush-compact-inc-dec-test
;  (cql-counter-inc-dec-test "flush and compact"
;                            {:nemesis (conductors/flush-and-compacter)}))

(def bridge-inc-test-bootstrap
  (cql-counter-inc-test "bridge bootstrap"
                        {:bootstrap (atom #{"n4" "n5"})
                         :nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-inc-test-bootstrap
  (cql-counter-inc-test "halves bootstrap"
                        {:bootstrap (atom #{"n4" "n5"})
                         :nemesis (nemesis/partition-random-halves)}))

(def isolate-node-inc-test-bootstrap
  (cql-counter-inc-test "isolate node bootstrap"
                        {:bootstrap (atom #{"n4" "n5"})
                         :nemesis (nemesis/partition-random-node)}))

(def crash-subset-inc-test-bootstrap
  (cql-counter-inc-test "crash bootstrap"
                        {:bootstrap (atom #{"n4" "n5"})
                         :nemesis (crash-nemesis)}))

;(def bridge-inc-dec-test-bootstrap
;  (cql-counter-inc-dec-test "bridge bootstrap"
;                            {:bootstrap (atom #{"n4" "n5"})
;                             :nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))
;
;(def halves-inc-dec-test-bootstrap
;  (cql-counter-inc-dec-test "halves bootstrap"
;                            {:bootstrap (atom #{"n4" "n5"})
;                             :nemesis (nemesis/partition-random-halves)}))
;
;(def isolate-node-inc-dec-test-bootstrap
;  (cql-counter-inc-dec-test "isolate node bootstrap"
;                            {:bootstrap (atom #{"n4" "n5"})
;                             :nemesis (nemesis/partition-random-node)}))
;
;(def crash-subset-inc-dec-test-bootstrap
;  (cql-counter-inc-dec-test "crash bootstrap"
;                            {:bootstrap (atom #{"n4" "n5"})
;                             :nemesis (crash-nemesis)}))

(def bridge-inc-test-decommission
  (cql-counter-inc-test "bridge decommission"
                        {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                         :decommissioner true}))

(def halves-inc-test-decommission
  (cql-counter-inc-test "halves decommission"
                        {:nemesis (nemesis/partition-random-halves)
                         :decommissioner true}))

(def isolate-node-inc-test-decommission
  (cql-counter-inc-test "isolate node decommission"
                        {:nemesis (nemesis/partition-random-node)
                         :decommissioner true}))

(def crash-subset-inc-test-decommission
  (cql-counter-inc-test "crash decommission"
                        {:client (cql-counter-client :quorum)
                         :nemesis (crash-nemesis)
                         :decommissioner true}))

;(def bridge-inc-dec-test-decommission
;  (cql-counter-inc-dec-test "bridge decommission"
;                            {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
;                             :decommissioner true}))
;
;(def halves-inc-dec-test-decommission
;  (cql-counter-inc-dec-test "halves decommission"
;                            {:nemesis (nemesis/partition-random-halves)
;                             :decommissioner true}))
;
;(def isolate-node-inc-dec-test-decommission
;  (cql-counter-inc-dec-test "isolate node decommission"
;                            {:nemesis (nemesis/partition-random-node)
;                             :decommissioner true}))
;
;(def crash-subset-inc-dec-test-decommission
;  (cql-counter-inc-dec-test "crash decommission"
;                            {:client (cql-counter-client :quorum)
;                             :nemesis (crash-nemesis)
;                             :decommissioner true}))

(def bridge-inc-test-mix
  (cql-counter-inc-test "bridge bootstrap and decommission"
                        {:bootstrap (atom #{"n5"})
                         :nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                         :decommissioner true}))

(def halves-inc-test-mix
  (cql-counter-inc-test "halves bootstrap and decommission"
                        {:bootstrap (atom #{"n5"})
                         :nemesis (nemesis/partition-random-halves)
                         :decommissioner true}))

(def isolate-node-inc-test-mix
  (cql-counter-inc-test "isolate node bootstrap and decommission"
                        {:bootstrap (atom #{"n5"})
                         :nemesis (nemesis/partition-random-node)
                         :decommissioner true}))

(def crash-subset-inc-test-mix
  (cql-counter-inc-test "crash bootstrap and decommission"
                        {:bootstrap (atom #{"n5"})
                         :nemesis (crash-nemesis)
                         :decommissioner true}))

;(def bridge-inc-dec-test-mix
;  (cql-counter-inc-dec-test "bridge bootstrap and decommission"
;                            {:bootstrap (atom #{"n5"})
;                             :nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
;                             :decommissioner true}))
;
;(def halves-inc-dec-test-mix
;  (cql-counter-inc-dec-test "halves bootstrap and decommission"
;                            {:bootstrap (atom #{"n5"})
;                             :nemesis (nemesis/partition-random-halves)
;                             :decommissioner true}))
;
;(def isolate-node-inc-dec-test-mix
;  (cql-counter-inc-dec-test "isolate node bootstrap and decommission"
;                            {:bootstrap (atom #{"n5"})
;                             :nemesis (nemesis/partition-random-node)
;                             :decommissioner true}))
;
;(def crash-subset-inc-dec-test-mix
;  (cql-counter-inc-dec-test "crash bootstrap and decommission"
;                            {:bootstrap (atom #{"n5"})
;                             :nemesis (crash-nemesis)
;                             :decommissioner true}))
