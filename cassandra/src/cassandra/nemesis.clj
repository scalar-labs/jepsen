(ns cassandra.nemesis
  (:require [clojure.set :as set]
            [jepsen
             [control :as c]
             [nemesis :as nemesis]
             [util    :as util :refer [meh]]]
            [cassandra
             [core       :as cass]
             [conductors :as conductors]]))

(defn safe-mostly-small-nonempty-subset
  "Returns a subset of the given collection, with a logarithmically decreasing
  probability of selecting more elements. Always selects at least one element.
      (->> #(mostly-small-nonempty-subset [1 2 3 4 5])
           repeatedly
           (map count)
           (take 10000)
           frequencies
           sort)
      ; => ([1 3824] [2 2340] [3 1595] [4 1266] [5 975])"
  [xs test]
  (-> xs
      count
      inc
      Math/log
      rand
      Math/exp
      long
      (take (shuffle xs))
      set
      (set/difference @(:bootstrap test))
      set
      (set/difference @(:decommission test))
      shuffle))

(defn test-aware-node-start-stopper
  "Takes a targeting function which, given a list of nodes, returns a single
  node or collection of nodes to affect, and two functions `(start! test node)`
  invoked on nemesis start, and `(stop! test node)` invoked on nemesis stop.
  Returns a nemesis which responds to :start and :stop by running the start!
  and stop! fns on each of the given nodes. During `start!` and `stop!`, binds
  the `jepsen.control` session to the given node, so you can just call `(c/exec
  ...)`.

  Re-selects a fresh node (or nodes) for each start--if targeter returns nil,
  skips the start. The return values from the start and stop fns will become
  the :values of the returned :info operations from the nemesis, e.g.:

      {:value {:n1 [:killed \"java\"]}}"
  [targeter start! stop!]
  (let [nodes (atom nil)]
    (reify nemesis/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (locking nodes
          (assoc op :type :info, :value
                 (case (:f op)
                   :start (if-let [ns (-> test :nodes (targeter test) util/coll)]
                            (if (compare-and-set! nodes nil ns)
                              (c/on-many ns (start! test (keyword c/*host*)))
                              (str "nemesis already disrupting " @nodes))
                            :no-target)
                   :stop (if-let [ns @nodes]
                           (let [value (c/on-many ns (stop! test (keyword c/*host*)))]
                             (reset! nodes nil)
                             value)
                           :not-started)))))

      (teardown! [this test]))))

(defn crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  []
  (test-aware-node-start-stopper
   safe-mostly-small-nonempty-subset
   (fn start [test node] (meh (c/su (c/exec :killall :-9 :java))) [:killed node])
   (fn stop  [test node] (meh (cass/guarded-start! node test)) [:restarted node])))

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
   :nemesis (crash-nemesis)})
