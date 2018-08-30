(ns cassandra.lww-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.lww :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

; These are commented out - they should fail!
; Present as a sanity check on the linearizability checker
(comment
  (deftest lww-bridge
           (run-test! bridge-test))

  (deftest lww-isolate-node
           (run-test! isolate-node-test))

  (deftest lww-halves
           (run-test! halves-test))

  (deftest lww-crash-subset
           (run-test! crash-subset-test)))
