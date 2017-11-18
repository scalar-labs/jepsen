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
  - Change Cassandra Java driver in project.clj
```
- [com.datastax.cassandra/cassandra-driver-core "3.0.2"]
+ [com.datastax.cassandra/cassandra-driver-core "3.1.3"]
```
  - Modify option methods as below in src/clojure/clojurewerkz/cassaforte/query.clj
```
diff --git src/clojure/clojurewerkz/cassaforte/query.clj src/clojure/clojurewerkz/cassaforte/query.clj
index 0899355..d4f80a1 100644
--- src/clojure/clojurewerkz/cassaforte/query.clj
+++ src/clojure/clojurewerkz/cassaforte/query.clj
@@ -578,7 +578,7 @@
                         (reduce
                          (fn [opts [option-name option-vals]]
                            ((resolve-create-keyspace-option option-name) opts option-vals))
-                         (.withOptions query-builder)
+                         (.with query-builder)
                          options))
        :if-not-exists (fn if-not-exists-query [query-builder _]
                         (.ifNotExists query-builder))}]
@@ -597,7 +597,7 @@
                         (reduce
                          (fn [opts [option-name option-vals]]
                            ((resolve-create-keyspace-option option-name) opts option-vals))
-                         (.withOptions query-builder)
+                         (.with query-builder)
                          options))}]
   (defn alter-keyspace
     [keyspace-name & statements]
```
  - Then, `lein install`

## Running Tests

> A whole category of tests can be run using the selectors defined in `project.clj`. For example, one could run `lein test :mv` to test materialized views. These tests are additive, so one could run `lein test :mv :lwt` to test materialized views and lightweight transactions.
> 
> To run an individual test, one can use a command like `lein test :only cassandra.counter-test/cql-counter-inc-halves`.
> 
> To test builds based on 3.0 or above, one needs to activate the `trunk` profile that contains a dependency on the patched version of Cassaforte described above. For example, the individual test from above can be run like `lein with-profile +trunk :only cassandra.counter-test/cql-counter-inc-halves`.
