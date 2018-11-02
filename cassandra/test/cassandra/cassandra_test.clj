(ns cassandra.cassandra-test
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [jepsen.core :as jepsen]
            [cassandra.collections.map :as map]
            [cassandra.collections.set :as set]
            [cassandra
             [core    :as cassandra]
             [batch   :as batch]
             [counter :as counter]
             [lwt     :as lwt]
             [nemesis :as can]
             [runner  :as runner]]))

(defn check
  [test-fn nemesis joining]
  (-> {:rf 3 :db (cassandra/db "3.11.3") :concurrency 5 :time-limit 60}
      (runner/combine-nemesis nemesis joining)
      test-fn
      jepsen/run!
      :results
      :valid?
      is))

(defmacro add-nemesis
  [name suffix test-fn joining]
  `(do
     (deftest ~(symbol (str name "-steady"      suffix)) (check ~test-fn `(can/none)                ~joining))
     (deftest ~(symbol (str name "-flush"       suffix)) (check ~test-fn `(can/flush-and-compacter) ~joining))
     ;(deftest ~(symbol (str name "-clock-drift" suffix)) (check ~test-fn `(can/clock)               ~joining))
     (deftest ~(symbol (str name "-bridge"      suffix)) (check ~test-fn `(can/bridge)              ~joining))
     (deftest ~(symbol (str name "-halves"      suffix)) (check ~test-fn `(can/halves)              ~joining))
     (deftest ~(symbol (str name "-isolation"   suffix)) (check ~test-fn `(can/isolation)           ~joining))
     (deftest ~(symbol (str name "-crash"       suffix)) (check ~test-fn `(can/crash)               ~joining))))

(defmacro def-tests
  [test-fn]
  (let [name# (-> test-fn name (string/replace "-test" ""))]
    `(do
       (add-nemesis name# ""                 ~test-fn {:name ""                 :bootstrap false :decommission false})
       (add-nemesis name# "-bootstrap"       ~test-fn {:name "-bootstrap"       :bootstrap true  :decommission false})
       (add-nemesis name# "-decommissioning" ~test-fn {:name "-decommissioning" :bootstrap false :decommission true})
       (add-nemesis name# "-rejoining"       ~test-fn {:name "-rejoining"       :bootstrap true  :decommission true}))))

(def-tests batch/batch-test)
(def-tests map/map-test)
(def-tests set/set-test)
(def-tests counter/cnt-inc-test)
(def-tests lwt/lwt-test)
