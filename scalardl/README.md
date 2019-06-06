# Jepsen tests for Scalar DL

The Scalar DL Jepsen tests make use of the [Cassandra Jepsen tests](https://github.com/scalar-labs/jepsen/tree/cassandra).

## How to run a test

1. Clone the `scalar` branch of this repository

    ```
    $ git clone -b scalar https://github.com/scalar-labs/jepsen.git
    ```
    
2. Copy `ledger.tar` to `jepsen/scalardl/ledger`.

3. Start Jepsen with docker

    - The script will start five nodes and a control node (jepsen-control)

    ```
    $ cd docker
    $ ./up.sh
    ```

    - Login to jepsen-control

    ```
    $ docker exec -it jepsen-control bash
    ```

4. Install the Cassandra test tool

    ```
    # in jepsen-control
    
    $ cd /jepsen/cassandra
    $ lein install
    ```

5. Run a Scalar DL Jepsen test

    ```
    # in jepsen-control

    $ cd /jepsen/scalardl
    $ lein run test --time-limit 60 --concurrency 20 --rate 5
    ```
    
    The `--rate` flag indicates how many requests per second will be performed, and `--concurrency` is the number of concurrent threads. At the moment we are running 10 threads per key so `--concurrency` needs to be set larger than 10.
    
    Use `lein run test --help` to see a list of the options.
