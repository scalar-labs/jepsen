# Cassandra tests with Jepsen

This is based on [riptano's jepsen](https://github.com/riptano/jepsen/tree/cassandra/cassandra).

## Current status
- Support Apache Cassandra 3.11.x
- Support `collections.map-test`, `collections.set-test`, `batch-test`, `counter-test`(only add) and `lwt-test`
  - Removed `lww-test` and `mv-test`

## How to test
### Start the Docker Container

- Add the following line to the Docker file of a node
  - Because C* needs Java 8

```diff
--- a/docker/node/Dockerfile-ubuntu
+++ b/docker/node/Dockerfile-ubuntu
@@ -7,4 +7,5 @@ RUN apt-get install -y openssh-server \
     curl faketime iproute2 iptables iputils-ping libzip4 \
     logrotate man man-db net-tools ntpdate psmisc python rsyslog \
     sudo unzip vim wget apt-transport-https \
+    && apt-get update && apt-get install -y openjdk-8-jre \
     && apt-get remove -y --purge --auto-remove systemd
```

- Run docker
```sh
cd ${JEPSEN}/docker
./up.sh --ubuntu
```

### Install Cassaforte
- Get and install `Cassaforte` which has been modified for the new Cassandra driver
  - Modified converter in `src/clojure/clojurewerkz/cassaforte/conversion.clj`
  - Modified option methods in `src/clojure/clojurewerkz/cassaforte/query.clj`

```sh
# In jepsen-control
cd ${WORKSPACE}
git clone -b driver-3.0-for-jepsen https://github.com/scalar-labs/cassaforte
cd cassaforte
lein install
```

### Run tests

`lein run test --test lwt --nemesis bridge --join bootstrap`

- See `lein run test --help` for full options
