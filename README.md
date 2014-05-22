# clj-rocksdb

clj-rocksdb provides idiomatic Clojure bindings (via the JNI) to RocksDB,
Facebook's embedded, persistent key-value store based on Google's
LevelDB. LevelDB and RocksDB share the same interface, so the bindings here
equally applicable to that project and are largely a refactoring of Factual's
[clj-leveldb](https://github.com/Factual/clj-leveldb).

## Usage

This is currently an early-commit, work-in-progress, so don't use this
yet. This project relies on a locally-bulit OS X binary of RocksDBJNI from
Fusesouce.

## TODO

Remaining work prior to the Alpha release:

0. Maven deployment for fusesource JNI RocksDB bindings -> migrate to
   Facebook's RocksDB Java bindings.
1. Add test coverage -> focused generative testing via test.check.
2. Add API's for RocksDB-specific features.
3. Add benchmarks -> FB benchmarks, but additional benchmarks and graphs under
   typical server traffic considerations.
4. Readme and rationale.

## License

Copyright © 2014 Bhaskar Mookerji and the 1611 λ-Calculus Club.

Distributed under the Eclipse Public License, the same as Clojure.
