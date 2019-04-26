(ns tidb.sets
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
    [knossos.op :as op]
    [tidb.sql :as c :refer :all]
    [tidb.basic :as basic]))

(defrecord SetClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (c/with-conn-failure-retry conn
      (c/execute! conn ["create table if not exists sets
                        (id     int not null primary key auto_increment,
                        value  bigint not null)"])))

  (invoke! [this test op]
    (c/with-error-handling op
      (c/with-txn-aborts op
        (case (:f op)
          :add  (do (c/insert! conn :sets (select-keys op [:value]))
                    (assoc op :type :ok))

          :read (->> (c/query conn ["select * from sets"])
                     (mapv :value)
                     (assoc op :type :ok, :value))))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn adds
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       (gen/seq)))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})

(defn workload
  [opts]
  (let [c (:concurrency opts)]
    {:client (SetClient. nil)
     :generator (->> (gen/reserve (/ c 2) (adds) (reads))
                     (gen/stagger 1/10))
     :checker (checker/set-full)}))
