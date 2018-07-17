(defproject cassandra "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Cassandra"
  :url "http://github.com/riptano/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/java.jmx "0.3.1"]
                 [org.clojars.jkni/jepsen "0.0.7-SNAPSHOT"]
                 [clojurewerkz/cassaforte "2.1.0-beta1"]
                 [com.codahale.metrics/metrics-core "3.0.2"]]
  :profiles {:dev {:plugins [[test2junit "1.1.1"]]}
             :trunk {:dependencies [[clojurewerkz/cassaforte "trunk-SNAPSHOT"]]}}
  :test-selectors {:steady :steady
                   :bootstrap :bootstrap
                   :map :map
                   :set :set
                   :mv :mv
                   :batch :batch
                   :lwt :lwt
                   :decommission :decommission
                   :counter :counter
                   :clock :clock
                   :all (constantly true)})
