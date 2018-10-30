(ns cassandra.conductors
  (:require [cassandra.core :as cassandra]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [jepsen [client :as client]
             [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]]))

(defn bootstrapper
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (let [bootstrap (:bootstrap test)
            decommission (:decommission test)]
        (if-let [node (first @bootstrap)]
          (do (info node "starting bootstrapping")
              (swap! bootstrap rest)
              (swap! decommission rest)
              (c/on node (cassandra/start! node test))
              (while (some #{cassandra/dns-resolve (name node)}
                           (cassandra/joining-nodes test))
                (info node "still joining")
                (Thread/sleep 1000))
              (assoc op :value (str node " bootstrapped")))
          (assoc op :value "no nodes left to bootstrap"))))
    (teardown! [this test] this)))

(defn decommissioner
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (let [decommission (:decommission test)
            bootstrap (:bootstrap test)]
        (if-let [node (some-> test cassandra/live-nodes (set/difference @decommission)
                         shuffle (get 3))] ; keep at least RF nodes
          (do (info node "decommissioning")
              (info @decommission "already decommissioned")
              (swap! decommission conj node)
              (swap! bootstrap conj node)
              (cassandra/nodetool node "decommission")
              (assoc op :value (str node " decommissioned")))
          (assoc op :value "no nodes eligible for decommission"))))
    (teardown! [this test] this)))

(defn replayer
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (let [live-nodes (cassandra/live-nodes test)]
        (doseq [node live-nodes]
          (cassandra/nodetool node "replaybatchlog"))
        (assoc op :value (str live-nodes " batch logs replayed"))))
    (teardown! [this test] this)))

(defn flush-and-compacter
  "Flushes to sstables and forces a major compaction on all nodes"
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op]
      (case (:f op)
        :start (do (doseq [node (:nodes test)]
                     (cassandra/nodetool node "flush")
                     (cassandra/nodetool node "compact"))
                   (assoc op :value (str (:nodes test) " nodes flushed and compacted")))
        :stop (assoc op :value "stop is a no-op with this nemesis")))
    (teardown! [this test] this)))

(defn mix-failure
  "Make a seq with start and stop for nemesis failure, and mix bootstrapping and decommissioning"
  [opts]
  (let [decop (when (:decommission (:join opts))
                {:type :info :f :decommission})
        bootop (when (:bootstrap (:join opts))
                 {:type :info :f :bootstrap})
        ops [decop bootop]
        base [(gen/sleep (+ (rand-int 30) 60))
              {:type :info :f :start}
              (gen/sleep (+ (rand-int 30) 60))
              {:type :info :f :stop}]]
    (if-let [op (rand-nth ops)]
      (conj base (gen/sleep (+ (rand-int 30) 60)) op)
      base)))

(defn mix-failure-seq
  [opts]
  (gen/seq (flatten (repeatedly #(mix-failure opts)))))


(defn std-gen
  [opts gen]
  (->> gen
       gen/mix
       (gen/stagger 1/5)
       (gen/nemesis
         (mix-failure-seq opts))
       (gen/time-limit (:time-limit opts))))
