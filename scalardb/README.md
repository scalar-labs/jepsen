# Jepsen tests for Scalar DB

This guide will teach you how to run Jepsen tests for Scalar DB.
The Scalar DB tests make use of the [Cassandra Jepsen tests](https://github.com/scalar-labs/jepsen/tree/cassandra).

## How to run a test

1. Clone the `scalar` branch of this repository

    ```
    $ git clone -b scalar https://github.com/scalar-labs/jepsen.git
    ```

2. Start Jepsen with docker

    - The script will start five nodes and a control node (jepsen-control)

    ```
    $ cd docker
    $ ./up.sh
    ```

    - Login to jepsen-control

    ```
    $ docker exec -it jepsen-control bash
    ```

3. Install the Cassandra test tool

    ```
    # in jepsen-control
    
    $ cd /jepsen/cassandra
    $ lein install
    ```

    Or, you can add the following line after `RUN cd /jepsen/jepsen && lein install` to `${JEPSEN}/docker/control/Dockerfile`

    ```
    RUN cd /jepsen/cassandra && lein install
    ```

4. Run a Scalar DB Jepsen test

    ```
    # in jepsen-control

    $ cd /jepsen/scalardb
    $ lein run test --test transfer --nemesis crash --join decommission --time-limit 300
    ```

    Use `lein run test --help` to see a list of the full options
