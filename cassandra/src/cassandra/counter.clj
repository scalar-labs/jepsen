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

(defrecord CQLCounterClient [tbl-created? conn writec]
  client/Client
  (open! [_ test _]
    (let [conn (cassandra/connect (->> test :nodes (map name)))]
      (CQLCounterClient. tbl-created? conn writec)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (cql/create-keyspace conn "jepsen_keyspace"
                             (if-not-exists)
                             (with {:replication
                                    {"class" "SimpleStrategy"
                                     "replication_factor" (:rf test)}}))
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
                    (where [[= :id 0]])))
      this))

  (invoke! [this test op]
    (cql/use-keyspace conn "jepsen_keyspace")
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

  (close! [_ _]
    (info "Closing client with conn" conn)
    (cassandra/disconnect! conn))

  (teardown! [_ _]))

(defn cql-counter-client
  "A counter implemented using CQL counters"
  ([] (CQLCounterClient. (atom false) nil :one))
  ([writec] (CQLCounterClient. (atom false) nil writec)))

(defn cnt-inc-test
  [opts]
  (merge (cassandra-test (str "counter-inc-"  (:suffix opts))
                         {:client (cql-counter-client)
                          :model nil
                          :generator (->> (repeat 100 add)
                                          (cons r)
                                          (conductors/std-gen opts))
                          :checker (checker/compose
                                    {:counter (checker/counter)})})
         opts))

;; TODO: check sub operations
;(defn cnt-inc-dec-test
;  [opts]
;  (merge (cassandra-test (str "counter-inc-dec-" (:suffix opts))
;                         {:client (cql-counter-client)
;                          :model nil
;                          :generator (->> (take 100 (cycle [add sub]))
;                                          (cons r)
;                                          (conductors/std-gen opts))
;                          :checker (checker/compose
;                                    {:counter (checker/counter)})})
;         opts))
