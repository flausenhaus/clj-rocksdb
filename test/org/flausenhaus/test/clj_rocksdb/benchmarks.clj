(ns org.flausenhaus.test.clj-rocksdb.benchmarks
  "Utilities for performance benchmarking, specifically against known
  performance known performance benchmarks from Facebook."
  (:require
   [clojure.test :refer [deftest testing]]
   [criterium.core :as bench]
   [org.flausenhaus.clj-rocksdb :as rdb]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Test Benchmark
;;;
;;; Some performance benchmarks based on what's here:
;;; https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks

(def ^:private db (rdb/mk-RocksDB))

(deftest ^:benchmark test-simple-bench-rw
  (testing "Simple reads"
    (bench/quick-bench (rdb/put! db "foo" "bar"))
    (bench/quick-bench (rdb/get db "foo"))))

;; 0. Bulk load of keys in random order.

;; 1. Bulk load of keys in sequential order.

;; 2. Random write.

;; 3. Random read.

;; 4. Multi-threaded read and single-threaded write.
