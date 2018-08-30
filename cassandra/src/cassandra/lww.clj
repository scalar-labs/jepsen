(ns cassandra.lww
  (:require [clojure [pprint :refer :all]
             [string :as str]]
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

(defrecord LwwCasRegisterClient [conn]
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
        (cql/create-table conn "lww_cas"
                          (if-not-exists)
                          (column-definitions {:id :int
                                               :value :int
                                               :primary-key [:id]}))
        ; @TODO change compaction storategy
        (cql/alter-table conn "lww_cas"
                          (with {:compaction-options (compaction-strategy)}))
        (->LwwCasRegisterClient conn))))
  (invoke! [this test op]
    (case (:f op)
      :cas (try (let [[v v'] (:value op)
                      read (-> (cassandra/execute
                                 conn
                                 "SELECT * FROM lww_cas WHERE id = 0;"
                                 :consistency-level (consistency-level :quorum))
                               first :value)]
                  (if (= v read)
                    (do (cassandra/execute
                          conn
                          (str "UPDATE lww_cas SET value = " v' " WHERE id = 0;")
                          :consistency-level (consistency-level :quorum))
                      (assoc op :type :ok))
                    (assoc op :type :fail)))
                (catch UnavailableException e
                  (assoc op :type :fail :error (.getMessage e)))
                (catch ReadTimeoutException e
                  (assoc op :type :fail :error :read-timed-out))
                (catch WriteTimeoutException e
                  (assoc op :type :fail :error :write-timed-out))
                (catch NoHostAvailableException e
                  (info "All the servers are down - waiting 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :error (.getMessage e))))
      :write (try (let [v' (:value op)]
                    (cassandra/execute
                      conn
                      (str "UPDATE lww_cas SET value = " v' " WHERE id = 0;")
                      :consistency-level (consistency-level :quorum))
                    (assoc op :type :ok))
                  (catch UnavailableException e
                    (assoc op :type :fail :error (.getMessage e)))
                  (catch ReadTimeoutException e
                    (assoc op :type :fail :error :read-timed-out))
                  (catch WriteTimeoutException e
                    (assoc op :type :fail :error :write-timed-out))
                  (catch NoHostAvailableException e
                    (info "All the servers are down - waiting 2s")
                    (Thread/sleep 2000)
                    (assoc op :type :fail :error (.getMessage e))))
      :read (try (let [value (-> (cassandra/execute
                                   conn
                                   "SELECT * FROM lww_cas WHERE id = 0;"
                                   :consistency-level (consistency-level :quorum))
                                 first :value)]
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

(defn lww-cas-register-client
  "A CAS register implemented using LWW client-side CAS"
  []
  (->LwwCasRegisterClient nil))

(defn lww-cas-register-test
  [name opts]
  (merge (cassandra-test (str "lww cas register " name)
                         {:client (lww-cas-register-client)
                          :model (model/cas-register)
                          :generator (gen/phases
                                      (->> [r w cas cas cas]
                                           (conductors/std-gen opts 60)))
                          :checker (checker/compose
                                    {:linear (checker/linearizable)})})
         (conductors/combine-nemesis opts)))

(def bridge-test
  (lww-cas-register-test "bridge"
                         {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-test
  (lww-cas-register-test "halves"
                         {:nemesis (nemesis/partition-random-halves)}))

(def isolate-node-test
  (lww-cas-register-test "isolate node"
                         {:nemesis (nemesis/partition-random-node)}))

(def crash-subset-test
  (lww-cas-register-test "crash"
                         {:nemesis (crash-nemesis)}))
