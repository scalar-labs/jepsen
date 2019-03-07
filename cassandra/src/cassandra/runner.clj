(ns cassandra.runner
  (:gen-class)
  (:require [jepsen
             [core    :as jepsen]
             [cli     :as jc]
             [nemesis :as jn]]
            [jepsen.nemesis.time :as nt]
            [cassandra.collections.map :as map]
            [cassandra.collections.set :as set]
            [cassandra [core       :as cassandra]
                       [batch      :as batch]
                       [counter    :as counter]
                       [lwt        :as lwt]
                       [conductors :as conductors]
                       [nemesis    :as can]]))

(def tests
  "A map of test names to test constructors."
  {"batch"   batch/batch-test
   "map"     map/map-test
   "set"     set/set-test
   "counter" counter/cnt-inc-test
   "lwt"     lwt/lwt-test})

(def nemeses
  {"none"      `(can/none)
   "flush"     `(can/flush-and-compacter)
   "bridge"    `(can/bridge)
   "halves"    `(can/halves)
   "isolation" `(can/isolation)
   "crash"     `(can/crash)})

(def joinings
  {"none"         {:name ""                 :bootstrap false :decommission false}
   "bootstrap"    {:name "-bootstrap"       :bootstrap true  :decommission false}
   "decommission" {:name "-decommissioning" :bootstrap false :decommission true}
   "rejoin"       {:name "-rejoining"       :bootstrap true  :decommission true}})

(def clocks
  {"none"   {:name ""              :bump false :strobe false}
   "bump"   {:name "-clock-bump"   :bump true  :strobe false}
   "strobe" {:name "-clock-strobe" :bump false :strobe true}
   "drift"  {:name "-clock-drift"  :bump true  :strobe true}})

(def opt-spec
  [(jc/repeated-opt nil "--test NAME" "Test(s) to run" [] tests)

   (jc/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                    [`(can/none)]
                    nemeses)

   (jc/repeated-opt nil "--join NAME" "Which node joinings to use"
                    [{:name "" :bootstrap false :decommission false}]
                    joinings)

   (jc/repeated-opt nil "--clock NAME" "Which clock-drift to use"
                    [{:name "" :bump false :strobe false}]
                    clocks)

   [nil "--rf REPLICATION_FACTOR" "Replication factor"
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   (jc/tarball-opt "http://www.us.apache.org/dist/cassandra/3.11.4/apache-cassandra-3.11.4-bin.tar.gz")])

(defn combine-nemesis
  "Combine nemesis options with bootstrapper and decommissioner"
  [opts nemesis joining clock]
  (-> opts
      (assoc :suffix (str (:name (eval nemesis)) (:name joining) (:name clock)))
      (assoc :join joining)
      (assoc :clock clock)
      (assoc :bootstrap
             (if (:bootstrap joining)
               (atom #{(last (:nodes opts))})
               (atom #{})))
      (assoc :decommission (atom #{}))
      (assoc :nemesis
             (jn/compose
               (conj {#{:start :stop} (:nemesis (eval nemesis))}
                     (when (:decommission joining)
                       {#{:decommission} (conductors/decommissioner)})
                     (when (:bootstrap joining)
                       {#{:bootstrap} (conductors/bootstrapper)})
                     (when (or (:bump clock) (:strobe clock))
                       {#{:reset :bump :strobe} (nt/clock-nemesis)}))))))

(defn test-cmd
   []
   {"test" {:opt-spec (into jc/test-opt-spec opt-spec)
            :opt-fn (fn [parsed] (-> parsed jc/test-opt-fn))
            :usage (jc/test-usage)
            :run (fn [{:keys [options]}]
                   (doseq [i        (range (:test-count options))
                           test-fn  (:test options)
                           nemesis  (:nemesis options)
                           joining  (:join options)
                           clock    (:clock options)]
                     (let [test (-> options
                                    (combine-nemesis nemesis joining clock)
                                    (assoc :db (cassandra/db (:cassandra options)))
                                    (dissoc :test)
                                    test-fn
                                    jepsen/run!)]
                       (when-not (:valid? (:results test))
                         (System/exit 1)))))}})

(defn -main
  [& args]
  (jc/run! (merge (jc/serve-cmd)
                  (test-cmd))
           args))
