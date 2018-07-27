# Cassandra

This is based on [riptano's jepsen](https://github.com/riptano/jepsen/tree/cassandra/cassandra).

## Current status
- Support only `collections.map-test` and `lwt-test`

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
  - Modify converter as below in src/clojure/clojurewerkz/cassaforte/conversion.clj
  - Modify option methods in src/clojure/clojurewerkz/cassaforte/query.clj

```
cd ~
git clone https://github.com/clojurewerkz/cassaforte.git
```

```
diff --git a/src/clojure/clojurewerkz/cassaforte/conversion.clj b/src/clojure/clojurewerkz/cassaforte/conversion.clj
index 46184b9..7427c33 100644
--- a/src/clojure/clojurewerkz/cassaforte/conversion.clj
+++ b/src/clojure/clojurewerkz/cassaforte/conversion.clj
@@ -32,18 +32,15 @@
                                               (if (< (inc i) (.size cd))
                                                 (recur (assoc row-data (keyword name) value) (inc i))
                                                 (assoc row-data (keyword name) value)))))))
-    (instance? Map java-val)       (let [t (transient {})]
-                                     (doseq [[k v] java-val]
-                                       (assoc! t k v))
-                                     (persistent! t))
-    (instance? Set java-val)       (let [t (transient #{})]
-                                     (doseq [v java-val]
-                                       (conj! t v))
-                                     (persistent! t))
-    (instance? List java-val)      (let [t (transient [])]
-                                     (doseq [v java-val]
-                                       (conj! t v))
-                                     (persistent! t))
+    (instance? Map java-val)       (persistent!
+                                     (reduce (fn [t [k v]] (assoc! t k v))
+                                             (transient {}) java-val))
+    (instance? Set java-val)       (persistent!
+                                     (reduce (fn [t v] (conj! t v))
+                                             (transient #{}) java-val))
+    (instance? List java-val)      (persistent!
+                                     (reduce (fn [t [k v]] (conj! t v))
+                                             (transient []) java-val))
     (instance? Host java-val)      (let [^Host host java-val]
                                      {:datacenter (.getDatacenter host)
                                       :address    (.getHostAddress (.getAddress host))
diff --git a/src/clojure/clojurewerkz/cassaforte/query.clj b/src/clojure/clojurewerkz/cassaforte/query.clj
index 0899355..d6fbcee 100644
--- a/src/clojure/clojurewerkz/cassaforte/query.clj
+++ b/src/clojure/clojurewerkz/cassaforte/query.clj
@@ -481,8 +481,7 @@
                          (fn [opts [option-name option-vals]]
                            ((resolve-alter-option option-name) opts option-vals))
                          (.withOptions query-builder)
-                         options)
-                        query-builder)
+                         options))
 
 
        :add-column    (fn add-column-statement [query-builder [column-name column-type]]
```

- Then, `lein install` Cassaforte

## Running Tests

> A whole category of tests can be run using the selectors defined in `project.clj`. For example, one could run `lein test :mv` to test materialized views. These tests are additive, so one could run `lein test :mv :lwt` to test materialized views and lightweight transactions.
> 
> To run an individual test, one can use a command like `lein test :only cassandra.counter-test/cql-counter-inc-halves`.
> 
> To test builds based on 3.0 or above, one needs to activate the `trunk` profile that contains a dependency on the patched version of Cassaforte described above. For example, the individual test from above can be run like `lein with-profile +trunk :only cassandra.counter-test/cql-counter-inc-halves`.
