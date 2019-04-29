# Cassandra tests with Jepsen

This is based on [riptano's jepsen](https://github.com/riptano/jepsen/tree/cassandra/cassandra).

## Current status
- Supports Apache Cassandra 3.11.x
- Support `collections.map-test`, `collections.set-test`, `batch-test`, `counter-test`(only add) and `lwt-test`
  - Removed `lww-test` and `mv-test`

## How to test

### Docker settings

You will probably need to increase the amount of memory that you make available to Docker in order to run these tests with Docker. We have had success using 8GB of memory and 2GB of swap. More, if you can spare it, is probably better.

### Start the Docker Container

- Fire up docker
```sh
cd ${JEPSEN}/docker
./up.sh
```

### Run tests

`lein run test --test lwt --nemesis bridge --join bootstrap`

See `lein run test --help` for full options
