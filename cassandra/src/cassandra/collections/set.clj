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

(defrecord CQLSetClient [tbl-created? conn writec]
  client/Client
  (open! [_ test _]
    (let [conn (cassandra/connect (->> test :nodes (map name)))]
      (CQLSetClient. tbl-created? conn writec)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (cql/create-keyspace conn "jepsen_keyspace"
                             (if-not-exists)
                             (with {:replication
                                    {"class" "SimpleStrategy"
                                     "replication_factor" (:rf test)}}))
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
                    (if-not-exists)))
      this))

  (invoke! [this test op]
    (cql/use-keyspace conn "jepsen_keyspace")
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

  (close! [_ _]
    (info "Closing client with conn" conn)
    (cassandra/disconnect! conn))

  (teardown! [_ _]))

(defn cql-set-client
  "A set implemented using CQL sets"
  ([] (CQLSetClient. (atom false) nil :one))
  ([writec] (CQLSetClient. (atom false) nil writec)))

(defn set-test
  [opts]
  (merge (cassandra-test (str "set-" (:suffix opts))
                         {:client (cql-set-client)
                          :model (model/set)
                          :generator (gen/phases
                                      (->> [(adds)]
                                           (conductors/std-gen opts))
                                      (read-once))
                          :checker (checker/compose
                                    {:set (checker/set)})})
         opts))
