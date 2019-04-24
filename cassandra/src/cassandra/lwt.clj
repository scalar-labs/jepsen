(ns cassandra.lwt
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

(def ak (keyword "[applied]")) ;this is the name C* returns, define now because
                               ;it isn't really a valid keyword from reader's
                               ;perspective

(defrecord CasRegisterClient [tbl-created? conn]
  client/Client
  (open! [_ test _]
    (let [conn (cassandra/connect (->> test :nodes (map name)))]
      (CasRegisterClient. tbl-created? conn)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (cql/create-keyspace conn "jepsen_keyspace"
                             (if-not-exists)
                             (with {:replication
                                    {"class" "SimpleStrategy"
                                     "replication_factor" (:rf test)}}))
        (cql/use-keyspace conn "jepsen_keyspace")
        (cql/create-table conn "lwt"
                          (if-not-exists)
                          (column-definitions {:id :int
                                               :value :int
                                               :primary-key [:id]}))
        ; @TODO change compaction storategy
        (cql/alter-table conn "lwt"
                          (with {:compaction-options (compaction-strategy)})))))

  (invoke! [this test op]
    (cql/use-keyspace conn "jepsen_keyspace")
    (case (:f op)
      :cas (try (let [[v v'] (:value op)
                      result (cql/update conn "lwt" {:value v'}
                                         (only-if [[= :value v]])
                                         (where [[= :id 0]]))]
                  (if (-> result first ak)
                    (assoc op :type :ok)
                    (assoc op :type :fail :value [v v'])))
                (catch UnavailableException e
                  (assoc op :type :fail :error (.getMessage e)))
                (catch ReadTimeoutException e
                  (assoc op :type :info :value :read-timed-out))
                (catch WriteTimeoutException e
                  (assoc op :type :info :value :write-timed-out))
                (catch NoHostAvailableException e
                  (info "All the servers are down - waiting 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :error (.getMessage e))))
      :write (try (let [v' (:value op)
                        result (cql/update conn
                                           "lwt"
                                           {:value v'}
                                           (only-if [[:in :value (range 5)]])
                                           (where [[= :id 0]]))]
                    (if (-> result first ak)
                      (assoc op :type :ok)
                      (let [result' (cql/insert conn "lwt" {:id 0
                                                            :value v'}
                                                (if-not-exists))]
                        (if (-> result' first ak)
                          (assoc op :type :ok)
                          (assoc op :type :fail)))))
                  (catch UnavailableException e
                    (assoc op :type :fail :error (.getMessage e)))
                  (catch ReadTimeoutException e
                    (assoc op :type :info :value :read-timed-out))
                  (catch WriteTimeoutException e
                    (assoc op :type :info :value :write-timed-out))
                  (catch NoHostAvailableException e
                    (info "All the servers are down - waiting 2s")
                    (Thread/sleep 2000)
                    (assoc op :type :fail :error (.getMessage e))))
      :read (try (let [value (->> (cassandra/execute conn
                                    "SELECT * FROM lwt WHERE id = 0;"
                                    :consistency-level (consistency-level :serial))
                                  first :value)]
                   (assoc op :type :ok :value value))
                 (catch UnavailableException e
                   (info "Not enough replicas - failing")
                   (assoc op :type :fail :error (.getMessage e)))
                 (catch ReadTimeoutException e
                   (assoc op :type :info :value :timed-out))
                 (catch NoHostAvailableException e
                   (info "All the servers are down - waiting 2s")
                   (Thread/sleep 2000)
                   (assoc op :type :fail :error (.getMessage e))))))

  (close! [_ _]
    (info "Closing client with conn" conn)
    (cassandra/disconnect! conn))

  (teardown! [_ _]))

(defn lwt-test
  [opts]
  (merge (cassandra-test (str "lwt-" (:suffix opts))
                         {:client (CasRegisterClient. (atom false) nil)
                          :model (model/cas-register)
                          :generator (gen/phases
                                      (->> [r w cas cas cas]
                                           (conductors/std-gen opts)))
                          :checker (checker/compose
                                    {:linear (checker/linearizable)})})
         opts))
