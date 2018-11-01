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

(defrecord BatchSetClient [tbl-created? conn]
  client/Client
  (open! [_ test _]
    (let [conn (cassandra/connect (->> test :nodes (map name)))]
      (BatchSetClient. tbl-created? conn)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
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
                          (with {:compaction-options (compaction-strategy)})))
      this))

  (invoke! [this test op]
    (cql/use-keyspace conn "jepsen_keyspace")
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
                                    (filter (fn [ret] (= (:cid ret) 0)))
                                    (map :value)
                                    (into (sorted-set)))
                       value-b (->> results
                                    (filter (fn [ret] (= (:cid ret) 1)))
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

  (close! [_ _]
    (info "Closing client with conn" conn)
    (cassandra/disconnect! conn))

  (teardown! [_ _]))

(defn batch-test
  [opts]
  (merge (cassandra-test (str "batch-set-" (:suffix opts))
                         {:client (BatchSetClient. (atom false) nil)
                          :model (model/set)
                          :generator (gen/phases
                                      (->> [(adds)]
                                           (conductors/std-gen opts))
                                      (gen/delay 5
                                                 (read-once)))
                          :checker (checker/compose
                                    {:set (checker/set)})})
         opts))
