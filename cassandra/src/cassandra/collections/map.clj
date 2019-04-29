(ns cassandra.collections.map
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [knossos.model :as model]
            [qbits.alia :as alia]
            [qbits.hayt]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all]
            [qbits.hayt.utils :refer [map-type]]
            [cassandra.core :refer :all]
            [cassandra.conductors :as conductors])
  (:import (clojure.lang ExceptionInfo)))

(defrecord CQLMapClient [tbl-created? conn writec]
  client/Client
  (open! [this test _]
    (let [cluster (alia/cluster {:contact-points (map name (:nodes test))})
          conn (alia/connect cluster)]
      (CQLMapClient. tbl-created? conn writec)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (alia/execute conn (create-keyspace :jepsen_keyspace
                                            (if-exists false)
                                            (with {:replication {"class"              "SimpleStrategy"
                                                                 "replication_factor" (:rf test)}})))
        (alia/execute conn (use-keyspace :jepsen_keyspace))
        (alia/execute conn (create-table :maps
                                         (if-exists false)
                                         (column-definitions {:id          :int
                                                              :elements    (map-type :int :int)
                                                              :primary-key [:id]})))
        (alia/execute conn (alter-table :maps (with {:compaction {:class :SizeTieredCompactionStrategy}})))
        (alia/execute conn (insert :maps (values [[:id 0]
                                                  [:elements {}]]))))))

  (invoke! [this test op]
    (try
      (alia/execute conn (use-keyspace :jepsen_keyspace))
      (case (:f op)
        :add (do (alia/execute conn
                               (update :maps
                                       (set-columns {:elements [+ {(:value op) (:value op)}]})
                                       (where [[= :id 0]]))
                               {:consistency-level writec})
                 (assoc op :type :ok))
        :read (do (wait-for-recovery 30 conn)
                  (let [value (->> (alia/execute conn
                                                 (select :maps (where [[= :id 0]]))
                                                 {:consistency  :all
                                                  :retry-policy aggressive-read})
                                   first
                                   :elements
                                   vals
                                   (into (sorted-set)))]
                    (assoc op :type :ok, :value value))))

      (catch ExceptionInfo e
        (assoc op :type :fail, :error (.getMessage e)))))

  (close! [_ _]
    (info "Closing client with conn" conn)
    (alia/shutdown conn))

  (teardown! [_ _]))

(defn cql-map-client
  "A set implemented using CQL maps"
  ([] (CQLMapClient. (atom false) nil :one))
  ([writec] (CQLMapClient. (atom false) nil writec)))

(defn map-test
  [opts]
  (merge (cassandra-test
           (str "map-" (:suffix opts))
           {:client    (cql-map-client)
            :model     (model/set)
            :generator (gen/phases
                         (->> [(adds)]
                              (conductors/std-gen opts))
                         (read-once))
            :checker   (checker/set)})
         opts))
