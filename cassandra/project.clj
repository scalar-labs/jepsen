(defproject cassandra "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Cassandra"
  :url "http://github.com/scalar-labs/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jmx "0.3.1"]
                 [jepsen "0.1.10-SNAPSHOT"]
                 [clojurewerkz/cassaforte "3.0.0-alpha2-SNAPSHOT"]]
  :profiles {:dev {:plugins [[test2junit "1.4.2"]]}
             :trunk {:dependencies [[clojurewerkz/cassaforte "3.0.0-alpha2-SNAPSHOT"]]}}
  :main cassandra.runner
  :aot [cassandra.runner])
