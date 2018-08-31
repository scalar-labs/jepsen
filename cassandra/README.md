# Cassandra tests with Jepsen

This is based on [riptano's jepsen](https://github.com/riptano/jepsen/tree/cassandra/cassandra).

## Current status
- Support Apache Cassandra 3.11.x
- Support `collections.map-test`, `collections.set-test`, `batch-test`, `counter-test`(only add) and `lwt-test`
  - All tests of `lww-test` are commented out because they should fail
  - WIP: `mv-test`

## How to test
### Start the Docker Container

```
cd ${JEPSEN}/docker
./up.sh
```

### Install Cassaforte
- Get and install `Cassaforte` which has been modified for the new Cassandra driver
  - Modified converter as below in src/clojure/clojurewerkz/cassaforte/conversion.clj
  - Modified option methods in src/clojure/clojurewerkz/cassaforte/query.clj

```
# In jepsen-control
cd ${WORKSPACE}
git clone -b driver-3.0-for-jepsen https://github.com/scalar-labs/cassaforte
cd cassaforte
lein install
```

### Run tests

> A whole category of tests can be run using the selectors defined in `project.clj`. For example, one could run `lein test :mv` to test materialized views. These tests are additive, so one could run `lein test :mv :lwt` to test materialized views and lightweight transactions.
> 
> To run an individual test, one can use a command like `lein test :only cassandra.counter-test/cql-counter-inc-halves`.

- If you add `-mix` as suffix, you can try a test with mixture of failure injection, bootstrapping and decommission

```
# In jepsen-control
cd ${JEPSEN}/cassandra
lein test :only cassandra.lwt-test/lwt-isolate-node-mix
```
