(ns cassandra.runner
  (:gen-class)
  (:require [jepsen
             [core    :as jepsen]
             [cli     :as jc]
             [nemesis :as jn]]
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
   "clock"     `(can/clock-drift)
   "bridge"    `(can/bridge)
   "halves"    `(can/halves)
   "isolation" `(can/isolation)
   "crash"     `(can/crash)})

(def joinings
  {"none"         {:name ""                 :bootstrap false :decommission false}
   "bootstrap"    {:name "-bootstrap"       :bootstrap true  :decommission false}
   "decommission" {:name "-decommissioning" :bootstrap false :decommission true}
   "rejoin"       {:name "-rejoining"       :bootstrap true  :decommission true}})

(def opt-spec
  [(jc/repeated-opt nil "--test NAME" "Test(s) to run" [] tests)

   (jc/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                    [`(can/none)]
                    nemeses)

   (jc/repeated-opt nil "--join NAME" "Which node joinings to use"
                    [{:name "" :bootstrap false :decommission false}]
                    joinings)

   [nil "--rf REPLICATION_FACTOR" "Replication factor"
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--cassandra VERSION" "C* version to use"
    :default "3.11.3"]])

(defn combine-nemesis
  "Combine nemesis options with bootstrapper and decommissioner"
  [opts nemesis joining]
  (-> opts
      (assoc :suffix (str (:name (eval nemesis)) (:name joining)))
      (assoc :join joining)
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
                       {#{:bootstrap} (conductors/bootstrapper)}))))))

(defn test-cmd
   []
   {"test" {:opt-spec (into jc/test-opt-spec opt-spec)
            :opt-fn (fn [parsed] (-> parsed jc/test-opt-fn))
            :usage (jc/test-usage)
            :run (fn [{:keys [options]}]
                   (doseq [i        (range (:test-count options))
                           test-fn  (:test options)
                           nemesis  (:nemesis options)
                           joining  (:join options)]
                     (let [test (-> options
                                    (combine-nemesis nemesis joining)
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
