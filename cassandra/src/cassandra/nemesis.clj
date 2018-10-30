(ns cassandra.nemesis
  (:require [jepsen
             [nemesis :as nemesis]]
            [cassandra
             [core :as cassandra]
             [conductors :as conductors]]))

;; empty nemesis
(defn none
  []
  {:name "steady"
   :nemesis nemesis/noop})

(defn flush-and-compacter
  []
  {:name "flush"
   :nemesis (conductors/flush-and-compacter)})

(defn clock-drift
  []
  {:name "clock-drift"
   :nemesis (nemesis/clock-scrambler 10000)})

(defn bridge
  []
  {:name "bridge"
   :nemesis (nemesis/partitioner (comp nemesis/bridge shuffle))})

(defn halves
  []
  {:name "halves"
   :nemesis (nemesis/partition-random-halves)})

(defn isolation
  []
  {:name "isolation"
   :nemesis (nemesis/partition-random-node)})

(defn crash
  []
  {:name "crash"
   :nemesis (cassandra/crash-nemesis)})
