(ns cassandra.batch
  (:require [cassandra.core :refer :all]
            [cassandra.conductors :as conductors]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [knossos.model :as model]
            [qbits.alia :as alia]
            [qbits.hayt]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all])
  (:import (clojure.lang ExceptionInfo)))

(defrecord BatchSetClient [table-created? conn]
  client/Client
  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (map name (:nodes test))})
          conn (alia/connect cluster)]
      (BatchSetClient. table-created? conn)))

  (setup! [_ test]

    (locking table-created?
      (when (compare-and-set! table-created? false true)
        (alia/execute conn (create-keyspace :jepsen_keyspace
                                            (if-exists false)
                                            (with {:replication {"class"              "SimpleStrategy"
                                                                 "replication_factor" (:rf test)}})))
        (alia/execute conn (use-keyspace :jepsen_keyspace))
        (alia/execute conn (create-table :bat
                                         (if-exists false)
                                         (column-definitions {:pid         :int
                                                              :cid         :int
                                                              :value       :int
                                                              :primary-key [:pid :cid]})))
        (alia/execute conn (alter-table :bat (with {:compaction {:class :SizeTieredCompactionStrategy}}))))))

  (invoke! [this test op]
    (try
      (alia/execute conn (use-keyspace :jepsen_keyspace))
      (case (:f op)
        :add (let [value (:value op)]
               (alia/execute conn
                             (str "BEGIN BATCH "
                                  "INSERT INTO bat (pid, cid, value) VALUES ("
                                  value ", 0," value "); "
                                  "INSERT INTO bat (pid, cid, value) VALUES ("
                                  value ", 1," value "); "
                                  "APPLY BATCH;")
                             {:consistency :quorum})
               (assoc op :type :ok))
        :read (let [results (alia/execute conn
                                          (select :bat)
                                          {:consistency-level :all
                                           :retry-policy      aggressive-read})
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
                  (assoc op :type :ok :value value-a))))

        (catch ExceptionInfo e
          (assoc op :type :fail, :error (.getMessage e)))))

    (close! [_ _]
            (info "Closing client conn" conn)
            (alia/shutdown conn))

    (teardown! [_ _]))

  (defn batch-test
    [opts]
    (merge (cassandra-test
             (str "batch-set-" (:suffix opts))
             {:client  (BatchSetClient. (atom false) nil)
              :model   (model/set)
              :checker (checker/set)
                       :generator (gen/phases
                                    (->> [(adds)]
                                         (conductors/std-gen opts))
                                    (gen/delay 5 (read-once)))})
           opts))
