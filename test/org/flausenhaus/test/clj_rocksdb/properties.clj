(ns org.flausenhaus.test.clj-rocksdb.properties
  "Defines utilities for performing property-based testing, specifically
  several properties of a key-value store.
  TODO (Buro): Finish this documentation.
      0. Roundtrip identity properties for serialization.
      1. Idempotent
"
  (:require
   [clojure.test :refer [deftest]]
   [clojure.test.check :as check]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as props]
   [expectations :refer :all]
   [taoensso.nippy :as nippy]
   [org.flausenhaus.clj-rocksdb :as rdb])
  (:import
   [java.util Arrays]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Test invariance/identity under serialization under different serialization
;;; roundtrips for:
;;;
;;; 0. IByteSerializable
;;; 1. byte-array into RocksDB
;;; 2. Types implementing IByteSerializable.

(def ^:private non-empty-byte-array (gen/such-that not-empty gen/bytes))

(def ^:private non-empty-any gen/any-printable)

(defprotocol IInvariant
  (^boolean invariant? [this transforms]
    "Is this invariant under serialization/deserialization?"))

(extend-protocol IInvariant
  (Class/forName "[B")
  (invariant? [this [transform inverse]]
    (java.util.Arrays/equals ^bytes this ^bytes (-> this transform inverse)))

  nil
  (invariant? [this _]
    true)

  java.lang.Object
  (invariant? [this [transform inverse]]
    (= this (-> this transform inverse))))

(expect true (invariant? (byte-array 10) [rdb/serialize rdb/deserialize]))
(expect true (invariant? nil [rdb/serialize rdb/deserialize]))
(expect true (invariant? (range 1 1000) [rdb/serialize rdb/deserialize]))

(defn- transform-invariance-property
  "Given a generator for a instance property (such as gen/bytes), generates a
  property test."
  [instance-generator transform]
  (props/for-all [instance instance-generator]
                 (invariant? instance transform)))

(test/defspec test-invariance-bytes
  100
  (transform-invariance-property non-empty-byte-array
                                 [rdb/serialize rdb/deserialize]))

(test/defspec test-invariance-any-printable
  40
  (transform-invariance-property non-empty-any
                                 [rdb/serialize rdb/deserialize]))

(test/defspec test-invariance-any-printable-standard
  40
  (rdb/with-reader pr-str read-string
    (transform-invariance-property non-empty-any
                                   [rdb/serialize rdb/deserialize])))

(def ^:private db (rdb/mk-RocksDB))

(test/defspec test-invariance-any-rocksdb
  40
  (transform-invariance-property non-empty-any
                                 [rdb/serialize rdb/deserialize]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Test key-value properties of collections and maps.
