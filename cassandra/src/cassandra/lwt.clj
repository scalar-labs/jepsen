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

(def ak (keyword "[applied]")) ;this is the name C* returns, define now because
                               ;it isn't really a valid keyword from reader's
                               ;perspective

(defrecord CasRegisterClient [conn]
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
        (cql/create-table conn "lwt"
                          (if-not-exists)
                          (column-definitions {:id :int
                                               :value :int
                                               :primary-key [:id]}))
        ; @TODO change compaction storategy
        (cql/alter-table conn "lwt"
                          (with {:compaction-options (compaction-strategy)}))
        (->CasRegisterClient conn))))
  (invoke! [this test op]
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
  (teardown! [_ _]
    (info "Tearing down client with conn" conn)
    (cassandra/disconnect! conn)))

(defn cas-register-client
  "A CAS register implemented using LWT"
  []
  (->CasRegisterClient nil))

(defn cas-register-test
  [name opts]
  (merge (cassandra-test (str "lwt register " name)
                         {:client (cas-register-client)
                          :model (model/cas-register)
                          :generator (gen/phases
                                      (->> [r w cas cas cas]
                                           (conductors/std-gen opts 60)))
                          :checker (checker/compose
                                    {:linear (checker/linearizable)})})
         (conductors/combine-nemesis opts)))

(def bridge-test
  (cas-register-test "bridge"
                     {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-test
  (cas-register-test "halves"
                     {:nemesis (nemesis/partition-random-halves)}))

(def isolate-node-test
  (cas-register-test "isolate node"
                     {:nemesis (nemesis/partition-random-node)}))

(def crash-subset-test
  (cas-register-test "crash"
                     {:nemesis (crash-nemesis)}))

(def flush-compact-test
  (cas-register-test "flush and compact"
                     {:nemesis (conductors/flush-and-compacter)}))

(def clock-drift-test
  (cas-register-test "clock drift"
                     {:nemesis (nemesis/clock-scrambler 10000)}))

(def bridge-test-bootstrap
  (cas-register-test "bridge bootstrap"
                     {:bootstrap (atom #{"n4" "n5"})
                      :nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))}))

(def halves-test-bootstrap
  (cas-register-test "halves bootstrap"
                     {:bootstrap (atom #{"n4" "n5"})
                      :nemesis (nemesis/partition-random-halves)}))

(def isolate-node-test-bootstrap
  (cas-register-test "isolate node bootstrap"
                     {:bootstrap (atom #{"n4" "n5"})
                      :nemesis (nemesis/partition-random-node)}))

(def crash-subset-test-bootstrap
  (cas-register-test "crash bootstrap"
                     {:bootstrap (atom #{"n4" "n5"})
                      :nemesis (crash-nemesis)}))

(def clock-drift-test-bootstrap
  (cas-register-test "clock drift bootstrap"
                     {:bootstrap (atom #{"n4" "n5"})
                      :nemesis (nemesis/clock-scrambler 10000)}))

(def bridge-test-decommission
  (cas-register-test "bridge decommission"
                     {:nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                      :decommissioner true}))

(def halves-test-decommission
  (cas-register-test "halves decommission"
                     {:nemesis (nemesis/partition-random-halves)
                      :decommissioner true}))

(def isolate-node-test-decommission
  (cas-register-test "isolate node decommission"
                     {:nemesis (nemesis/partition-random-node)
                      :decommissioner true}))

(def crash-subset-test-decommission
  (cas-register-test "crash decommission"
                     {:nemesis (crash-nemesis)
                      :decommissioner true}))

(def clock-drift-test-decommission
  (cas-register-test "clock drift decommission"
                     {:nemesis (nemesis/clock-scrambler 10000)
                      :decommissioner true}))

(def bridge-test-mix
  (cas-register-test "bridge bootstrap and decommission"
                     {:bootstrap (atom #{"n5"})
                      :nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))
                      :decommissioner true}))

(def halves-test-mix
  (cas-register-test "halves bootstrap and decommission"
                     {:bootstrap (atom #{"n5"})
                      :nemesis (nemesis/partition-random-halves)
                      :decommissioner true}))

(def isolate-node-test-mix
  (cas-register-test "isolate node  bootstrap and decommission"
                     {:bootstrap (atom #{"n5"})
                      :nemesis (nemesis/partition-random-node)
                      :decommissioner true}))

(def crash-subset-test-mix
  (cas-register-test "crash bootstrap and decommission"
                     {:bootstrap (atom #{"n5"})
                      :nemesis (crash-nemesis)
                      :decommissioner true}))

(def clock-drift-test-mix
  (cas-register-test "clock drift bootstrap and decommission"
                     {:bootstrap (atom #{"n5"})
                      :nemesis (nemesis/clock-scrambler 10000)
                      :decommissioner true}))
