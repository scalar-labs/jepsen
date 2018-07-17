# Cassandra

This is based on [riptano's jepsen](https://github.com/riptano/jepsen/tree/cassandra/cassandra).

## Current status
- Support only `lwt` tests

## Starting the Docker Container

```
cd ${JEPSEN}/docker
./up.sh
```

- If you use Docker for MacOS, you can increase memory limit
```
VBoxManage modifyvm default --memory 4096
```

## Environment Setup (within Docker container)

- Change Cassandra wrapper `Cassaforte` for Cassandra 3.x
  - Cassaforte
```
cd ~
git clone https://github.com/clojurewerkz/cassaforte.git
```
  - Modify option methods as below in src/clojure/clojurewerkz/cassaforte/query.clj
```
diff --git a/src/clojure/clojurewerkz/cassaforte/query.clj b/src/clojure/clojurewerkz/cassaforte/query.clj
index 0899355..1a4783c 100644
--- a/src/clojure/clojurewerkz/cassaforte/query.clj
+++ b/src/clojure/clojurewerkz/cassaforte/query.clj
@@ -481,8 +481,7 @@
                          (fn [opts [option-name option-vals]]
                            ((resolve-alter-option option-name) opts option-vals))
                          (.withOptions query-builder)
-                         options)
-                        query-builder)
+                         options))
```
  - Then, `lein install`

## Running Tests

> A whole category of tests can be run using the selectors defined in `project.clj`. For example, one could run `lein test :mv` to test materialized views. These tests are additive, so one could run `lein test :mv :lwt` to test materialized views and lightweight transactions.
> 
> To run an individual test, one can use a command like `lein test :only cassandra.counter-test/cql-counter-inc-halves`.
> 
> To test builds based on 3.0 or above, one needs to activate the `trunk` profile that contains a dependency on the patched version of Cassaforte described above. For example, the individual test from above can be run like `lein with-profile +trunk :only cassandra.counter-test/cql-counter-inc-halves`.
