(ns cassandra.conductors
  (:require [cassandra.core :as cassandra]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [jepsen [client :as client]
             [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]
             [net :as net]
             [util :as util :refer [meh]]]))

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

; TODO: Modify to support mv_test
;(defn flexible-partitioner
;  "Responds to a :start operation by cutting network links as defined by
;  (grudge nodes), and responds to :stop by healing the network. Uses
;  :grudge entry in op to determine grudge function used."
;  [grudge-map]
;  (reify client/Client
;    (setup! [this test _]
;      (net/heal! (:net test) test)
;      this)
;
;    (invoke! [this test op]
;      (case (:f op)
;        :start (let [grudge ((-> op :grudge grudge-map) (:nodes test))]
;                 (nemesis/partition! test grudge)
;                 (assoc op :value (str "Cut off " (pr-str grudge))))
;        :stop  (do (net/heal! (:net test) test)
;                   (assoc op :value "fully connected"))))
;
;    (teardown! [this test]
;      (net/heal! (:net test) test))))

(defn mix-failure
  "Make a seq with start and stop for nemesis failure, and mix bootstrapping and decommissioning"
  [opts]
  (let [decop (when (:decommissioner opts)
                {:type :info :f :decommission})
        bootop (when (and (:bootstrap opts) (not-empty @(:bootstrap opts)))
                 {:type :info :f :bootstrap})
        ops [decop bootop]]
    (concat
      (->> (repeatedly #(vector (gen/sleep (+ (rand-int 20) 1))
                                {:type :info :f :start}
                                (gen/sleep (+ (rand-int 20) 1))
                                (rand-nth ops)
                                (gen/sleep (+ (rand-int 20) 1))
                                {:type :info :f :stop}))
           (take 60) flatten)
      [(gen/sleep (+ (rand-int 20) 1))])))

(defn mix-failure-seq
  [opts]
  (gen/seq (cycle (mix-failure opts))))

(defn combine-nemesis
  "Combine nemesis options with bootstrapper and decommissioner"
  [opts]
  (assoc opts :nemesis
         (nemesis/compose
           (conj {#{:start :stop} (:nemesis opts)}
                 (when (:decommissioner opts)
                   {#{:decommission} (decommissioner)})
                 (when (and (:bootstrap opts) (not-empty @(:bootstrap opts)))
                   {#{:bootstrap} (bootstrapper)})))))

(defn std-gen
  ([opts gen] (std-gen opts 300 gen))
  ([opts duration gen]
   (->> gen
        gen/mix
        (gen/stagger 1/5)
        (gen/delay 2)
        (gen/nemesis
          (mix-failure-seq opts))
        (gen/time-limit duration))))
