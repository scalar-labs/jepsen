(ns cassandra.lwt
  (:require [cassandra.core :refer :all]
            [cassandra.conductors :as conductors]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [checker   :as checker]
             [client    :as client]
             [generator :as gen]]
            [knossos.model :as model]
            [qbits.alia :as alia]
            [qbits.hayt]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all])
  (:import (clojure.lang ExceptionInfo)))

(def ak (keyword "[applied]")) ; this is the name C* returns, define now because
                               ; it isn't really a valid keyword from reader's
                               ; perspective

(defrecord CasRegisterClient [tbl-created? conn]
  client/Client
  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (map name (:nodes test))})
          conn (alia/connect cluster)]
      (CasRegisterClient. tbl-created? conn)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (alia/execute conn (create-keyspace :jepsen_keyspace
                                            (if-exists false)
                                            (with {:replication {"class"              "SimpleStrategy"
                                                                 "replication_factor" (:rf test)}})))
        (alia/execute conn (use-keyspace :jepsen_keyspace))
        (alia/execute conn (create-table :lwt
                                         (if-exists false)
                                         (column-definitions {:id          :int
                                                              :value       :int
                                                              :primary-key [:id]})))
        (alia/execute conn (alter-table :lwt (with {:compaction {:class :SizeTieredCompactionStrategy}}))))))

  (invoke! [this test op]
    (try
      (alia/execute conn (use-keyspace :jepsen_keyspace))
      (case (:f op)
        :cas (let [[old new] (:value op)
                   result (alia/execute conn (update :lwt
                                                     (set-columns {:value new})
                                                     (only-if [[= :value old]])
                                                     (where [[= :id 0]])))]
               (if (-> result first ak)
                 (assoc op :type :ok)
                 (assoc op :type :fail, :value [old new])))
        :write (let [v' (:value op)
                     result (alia/execute conn (update :lwt
                                                       (set-columns {:value v'})
                                                       (only-if [[:in :value (range 5)]])
                                                       (where [[= :id 0]])))]
                 (if (-> result first ak)
                   (assoc op :type :ok)
                   (let [result' (alia/execute conn (insert :lwt
                                                            (values [[:id 0]
                                                                     [:value v']])
                                                            (if-exists false)))]
                     (if (-> result' first ak)
                       (assoc op :type :ok)
                       (assoc op :type :fail)))))
        :read (let [value (->> (alia/execute conn
                                             (select :lwt (where [[= :id 0]]))
                                             {:consistency :serial})
                               first
                               :value)]
                (assoc op :type :ok, :value value)))

      (catch ExceptionInfo e
        (assoc op :type :fail, :error (.getMessage e)))))

  (close! [_ _]
    (info "Closing client with conn" conn)
    (alia/shutdown conn))

  (teardown! [_ _]))

(defn lwt-test
  [opts]
  (merge (cassandra-test
           (str "lwt-" (:suffix opts))
           {:client    (CasRegisterClient. (atom false) nil)
            :checker   (checker/linearizable {:model     (model/cas-register)
                                              :algorithm :linear})
            :generator (gen/phases
                         (->> [r w cas cas cas]
                              (conductors/std-gen opts)))})
         opts))
