(ns cassandra.lww-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cassandra.lww :refer :all]
            [cassandra.core-test :refer :all]
            [jepsen [core :as jepsen]
             [report :as report]]))

; These are commented out - they should fail!
; Present as a sanity check on the linearizability checker
(comment (deftest lwt-test-bridge
           (run-cas-register-test! bridge-test timestamp))

         (deftest lwt-test-isolate-node
           (run-cas-register-test! isolate-node-test timestamp))

         (deftest lwt-test-halves
           (run-cas-register-test! halves-test timestamp))

         (deftest lwt-test-crash-subset
           (run-cas-register-test! crash-subset-test timestamp)))
