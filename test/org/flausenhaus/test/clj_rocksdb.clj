(ns org.flausenhaus.test.clj-rocksdb
  (:refer-clojure :exclude [get])
  (:require
   [clojure.test :refer [deftest]]
   [clojure.test.check.clojure-test :as test]
   [expectations :refer :all]
   [org.flausenhaus.clj-rocksdb :as rdb]
   [org.flausenhaus.test.clj-rocksdb.properties :as rdb-props])
  (:import
   [org.flausenhaus.clj_rocksdb
    Batch
    CloseableSeq
    RocksDB]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Demo tests to demonstrate properties and usage

(def ^:private db (rdb/mk-RocksDB))
(expect (more-> RocksDB identity
                Batch rdb/batch)
        db)
(expect true (from-each [protocol [rdb/IPersistentKVFactory
                                   rdb/IPersistentKVRead
                                   rdb/IPersistentKVWrite]]
                        (satisfies? protocol db)))

(expect nil? (rdb/put! db "foo" "bar"))
(expect "bar" (rdb/get db "foo"))
(rdb/put! db "baz" "bazbar")
(expect nil? (rdb/iterator db))
(expect CloseableSeq (rdb/iterator db "foo"))
(expect [["baz" "bazbar"] ["foo" "bar"]] (into [] (rdb/iterator db "foo")))
(expect "bar" (clojure.core/get db "foo"))
(expect "bazbar" (clojure.core/get db "baz"))
(expect "default" (clojure.core/get db "bar" "default"))
(expect nil? (rdb/delete! db "foo"))
(expect nil? (rdb/get db "foo" nil))
