(ns org.flausenhaus.clj-rocksdb
  "Idiomatic Clojure bindings for the C++ RocksDB library, an embedded
   persistent key-value store for flash storage and server workloads based on
   LevelDB. More details about RocksDB are given at
   https://github.com/facebook/rocksdb/wiki/Rocksdb-Architecture-Guide.
   The implementation here borrows heavily from Factual's clj-leveldb.

   This namespace provides a few things:
   0. Protocols/type definitions: byte serialization (IByteSerializable),
   closeable sequences (CloseableSeq), and mutable
   datastores (IPersistentKVFactory/IPersistentKVRead/IPersistentKVWrite).  Key
   and values are by default serialized through Nippy, can be be rebound using
   your serialization/deserialization functiosn of choice by using
   with-deserializer and with-serializer. RocksDB exposes a batch writes and
   snapshots in addition to expected read/write access.

   1. RocksDB-backed caching and memoization: RocksDB's bindings implement
   clojure.core.cache.CacheProtocol, and provide memoization through
   clojure.core.memoize.

   Examples!:
   ;; Creates a RocksDB instance with sane defaults in a temp directory and
   ;; test read/write.
   (require '[org.flausenhaus.clj-rocksdb :as rdb])
   (def db (rdb/mk-RocksDB))
   (rdb/put! db \"foo\" \"bar\")
   (rdb/get db \"foo\")

   For more examples see the test/org/flausenhaus/test/rocksdb.clj."
  (:refer-clojure :exclude [get])
  (:require
   [me.raynes.fs :as fs]
   [taoensso.nippy :as nippy]
   [clojure.java.io :as io])
  (:import
   [java.io
    File
    Closeable]
   [java.util
    Arrays]
   [org.fusesource.rocksdbjni
    JniDBFactory]
   [org.iq80.leveldb
    CompressionType
    DB
    DBFactory
    DBIterator
    Options
    Range
    ReadOptions
    WriteOptions
    WriteBatch]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Serialization and Sequence Utilities=

;; By default, we use nippy for serialization.
(def ^:dynamic *serializer* nippy/freeze)
(def ^:dynamic *deserializer* nippy/thaw)

;; TODO (Buro): Make

(defmacro with-serializer
  "Evaluates a body using the provided serializer."
  [serializer & body]
  `(binding [*serializer* ~serializer]
     ~@body))

(defmacro with-deserializer
  "Evaluates body using the provided deserializer."
  [deserializer & body]
  `(binding [*deserializer* ~deserializer]
     ~@body))

(defmacro with-reader
  "Utitily composing with-serializer and with-deserializer: given a both a
  serializer and deserializer, evaluates body."
  [serializer deserializer & body]
  `(with-serializer ~serializer
     (with-deserializer ~deserializer
       ~@body)))

(defprotocol IByteSerializable
  "Serialize and deserialize Clojure to byte-arrays.

   By contract, this should return a byte-array.  Note that if you want to just
   use a different serializer or deserializer, it should be more convenient to
   use the with-* macros in this namespace to consume the *serializer* and
   *deserializer* binding"
  (^bytes serialize [obj]
    "Binary serialize.")
  (^bytes deserialize [obj]
    "Binary deserialize."))

(extend-protocol IByteSerializable
  nil
  (serialize [_] (*serializer* nil))
  (deserialize [_] nil)

  java.lang.Object
  (serialize [obj] (*serializer* obj))
  (deserialize [obj] (*deserializer* obj)))

(deftype CloseableSeq [impl-seq close-fn]
  clojure.lang.ISeq
  clojure.lang.Sequential
  clojure.lang.Seqable
  clojure.lang.IPersistentCollection
  (equiv [this obj]
    (loop [a this b obj]
      (if (or (empty? a) (empty? b))
        (and (empty? a) (empty? b))
        (if (= (first obj) (first b))
          (recur (rest a) (rest b))
          false))))
  (count [this]
    (count impl-seq))
  (first [this]
    (if-let [f (first impl-seq)]
      f
      (.close this)))
  (next [this]
    (if-let [n (next impl-seq)]
      (CloseableSeq. n close-fn)
      (.close this)))
  (cons [this obj]
    (CloseableSeq. (cons obj impl-seq) close-fn))
  (more [this]
    (if-let [n (next this)]
      n
      '()))
  (empty [this]
    (CloseableSeq. '() close-fn))
  (seq [this]
    this)

  java.io.Closeable
  (close [_]
    (close-fn)))

(defn- closeable-seq
  "Given an underlying sequence and a closeable function, creates a sequence
  that can be closed when exhausted."
  [impl-seq close-fn]
  (if (seq impl-seq)
    (->CloseableSeq impl-seq close-fn)
    (do (close-fn)
        nil)))

(defn- ba=
  "Compare two byte-arrays for equality."
  [^bytes x ^bytes y]
  (java.util.Arrays/equals x y))

(defn- iterator-seq-
  "Creates a closeable iterator a DBIterator given and start and end keys."
  [^DBIterator iterator start end]
  (when start
    (.seek iterator (serialize start))
    (.seekToFirst iterator))
  (let [iter (iterator-seq iterator)
        iter (let [end (serialize end)]
               (if (ba= end (serialize nil))
                 iter
                 (take-while #(ba= end ^bytes (key %)) iter)))
        impl-seq (map #(vector (deserialize (key %)) (deserialize (val %))) iter)
        close-fn #(.close iterator)]
    (closeable-seq impl-seq close-fn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; RocksDB bindings: an overall protocol capturing the API for simple
;;; kv-datstore with mutable state.

(defprotocol IPersistentKVFactory
  "An interface for creating to a persistent key-value datastore with mutable
  state. To prevent resource leakage, any type implementing IPersistentKVWrite
  should also implement java.lang.Closeable.

  TODO: Add more documentation here about the argument types."
  (open [this file-handle options]
    "Opens a DB at the specified file handle and returns kv-datastore.")
  (destroy [this file-handle options]
    "Destroys DB at file-handle.")
  (repair [this file-handle options]
    "Attempts to reconstruct DB from file"))

(defprotocol IPersistentKVRead
  "An interface for reading a persistent key-value datastore with mutable
  state. To prevent resource leakage, any type implementing IPersistentKVRead
  should also implement java.lang.Closeable.

  TODO: Add more documentation here about the argument types."
  (iterator [this] [this start] [this start end]
    "Returns a CloseableSeq of map entries ranging from start to
    end. Automagically closes when exhausted.")
  (snapshot [this]
    "Returns a snapshot of the database that can be read and iterated
    over. This needs to be closed explicitly.")
  (stats [this property]
    "Returns statistics for the database.")
  (bounds [this]
    "Returns a tuple of the lower and upper keys in the database or snapshot.")
  (get [this key] [this key default]
    "Returns the value of key. If the key doesn't exist, returns default or
    nil."))

(defprotocol IPersistentKVWrite
  "An interface for writing to a persistent key-value datastore with mutable
  state. To prevent resource leakage, any type implementing IPersistentKVWrite
  should also implement java.lang.Closeable.

  TODO: Add more documentation here about the argument types."
  (batch [this] [this write-options]
    "Returns a batch writer for bulk writing key-value pairs atomically. Needs
    to be closed explicitly.")
  (sync! [this]
    "Force a sync/write to disk.")
  (compact! [this] [this start] [this start end]
    "Forces compaction.")
  (put! [this key value]
    "Put a key-value pair.")
  (put-all! [this kvs]
    "(Batch) put a map of key-value pairs atomically.")
  (delete! [this key] [this key value]
    "Delete a key. Optionally, given a value, only delete if retieved value is equal.")
  (delete-all! [this]
    "Delete all entries."))

(deftype Snapshot [^DB store ^ReadOptions read-options]
  IPersistentKVRead
  (iterator [this]
    (iterator this nil nil))
  (iterator [this start]
    (iterator this start nil))
  (iterator [this start end]
    (if read-options
      (iterator-seq- (.iterator store read-options) start end)
      (iterator-seq- (.iterator store) start end)))
  (bounds [this]
    (with-open [^DBIterator iterator (.iterator store read-options)]
      (when (.hasNext (doto iterator .seekToFirst))
        [(-> (doto iterator .seekToFirst) .peekNext key deserialize)
         (-> (doto iterator .seekToLast) .peekNext key deserialize)])))
  (get [this key]
    (get this key nil))
  (get [this key default]
    (let [key (serialize key)
          val (deserialize (try
                             (if read-options
                               (.get store key read-options)
                               (.get store key))
                             (catch NullPointerException e
                               nil)))]
      (if val val default)))
  (snapshot [this]
    this)

  java.io.Closeable
  (close [this]
    (-> read-options .snapshot .close)))

(deftype Batch [^DB store ^WriteBatch batch ^WriteOptions write-options]
  IPersistentKVWrite
  (put! [this key value]
    (.put batch (serialize key) (serialize value)))
  (delete! [this key]
    (.delete batch (serialize key)))
  (batch [this]
    this)
  (batch [this _]
    this)

  java.io.Closeable
  (close [this]
    (if write-options
      (.write store batch write-options)
      (.write store batch))
    (.close batch)))

(deftype RocksDB [^DB store ^ReadOptions read-options ^WriteOptions write-options ^File file]
  ;; TODO: Actually test this factory bit out.
  IPersistentKVFactory
  (open [this file-handle options]
    (assert false "Not used yet.")
    (.open JniDBFactory/factory file (Options.)))
  (destroy [this file-handle options]
    (.destroy JniDBFactory/factory file (Options.)))
  (repair [this file-handle options]
    (.repair JniDBFactory/factory file (Options.)))

  IPersistentKVRead
  (iterator [this]
    (iterator this nil nil))
  (iterator [this start]
    (iterator this start nil))
  (iterator [this start end]
    (iterator-seq- (.iterator store) start end))
  (snapshot [this]
    (->Snapshot this (doto (ReadOptions.) (.snapshot (.getSnapshot store)))))
  (stats [this property]
    (.getProperty store "rockdb.stats"))
  (bounds [this]
    (with-open [^DBIterator iterator (.iterator store read-options)]
      (when (.hasNext (doto iterator .seekToFirst))
        [(-> (doto iterator .seekToFirst) .peekNext key deserialize)
         (-> (doto iterator .seekToLast) .peekNext key deserialize)])))
  (get [this key]
    (get this key nil))
  (get [this key default]
    (let [key (serialize key)
          val (deserialize (try
                             (if read-options
                               (.get store key read-options)
                               (.get store key))
                             (catch NullPointerException e
                               nil)))]
      (if val val default)))

  IPersistentKVWrite
  (batch [this]
    (->Batch store (.createWriteBatch store) write-options))
  (batch [this options]
    (->Batch store (.createWriteBatch store) options))
  (sync! [this]
    (with-open [^Batch batch (batch this (doto (WriteOptions.) (.sync true)))]))
  (compact! [this]
    (let [[start end] (bounds this)]
      (compact! this start end)))
  (compact! [this start]
    (let [[_ end] (bounds this)]
      (compact! this start end)))
  (compact! [this start end]
    (.compactRange store (serialize start) (serialize end)))
  (put! [this key value]
    (let [k (serialize key)
          v (serialize value)]
      (if write-options
        (.put store k v write-options)
        (.put store k v))))
  (put-all! [this kvs]
    (with-open [^Batch batch (batch this)]
      (doseq [[key value] kvs
              :let [k (serialize key)
                    v (serialize value)]]
        (put! batch k v))))
  (delete! [this key]
    (let [k (serialize key)]
      (if write-options
        (.delete store k write-options)
        (.delete store k))))
  (delete! [this key value]
    (assert false "not implemented yet"))
  (delete-all! [this]
    (assert false "not implemented yet"))

  java.io.Closeable
  (close [this]
    (.close store))

  ;; TODO (Buro): Actually make this countable.
  clojure.lang.Counted
  (count [this]
    (->> this
         bounds
         (map serialize)
         (apply #(Range. %1 %2))
         into-array
         (.getApproximateSizes store)
         first))

  clojure.lang.Seqable
  (seq [this]
    ;; TODO (Buro): Actually make this seq-able.
    (for [[k v] (iterator this nil nil)]
      (clojure.lang.MapEntry. (deserialize k) (deserialize v))))

  clojure.lang.ILookup
  (valAt [this key]
    (get this key nil))
  (valAt [this key not-found]
    (if-let [val (get this key nil)]
      val
      not-found)))

;; TODO (Buro): Add more options http://godoc.org/github.com/alberts/gorocks
(def ^:private option-setters
  {:create-if-missing? #(.createIfMissing ^Options %1 %2)
   :error-if-exists?   #(.errorIfExists ^Options %1 %2)
   :write-buffer-size  #(.writeBufferSize ^Options %1 %2)
   :block-size         #(.blockSize ^Options %1 %2)
   :block-restart-interval #(.blockRestartInterval ^Options %1 %2)
   :max-open-files     #(.maxOpenFiles ^Options %1 %2)
   :cache-size         #(.cacheSize ^Options %1 %2)
   :comparator         #(.comparator ^Options %1 %2)
   :paranoid-checks?   #(.paranoidChecks ^Options %1 %2)
   :compress?          #(.compressionType ^Options %1 (if % CompressionType/SNAPPY CompressionType/NONE))
   :logger             #(.logger ^Options %1 %2)})

(defn mk-RocksDB
  "Creates a closeable database object, which takes a directory and zero or
   more options and implements both IPersistentKVRead and IPersistentKVWrite."
  [& {:keys [create-if-missing?
             error-if-exists?
             write-buffer-size
             block-size
             max-open-files
             cache-size
             comparator
             compress?
             paranoid-checks?
             block-restart-interval
             logger
             directory]
      :or {compress? true
           cache-size (* 32 1024 1024)
           block-size (* 16 1024)
           write-buffer-size (* 32 1024 1024)
           create-if-missing? true
           error-if-exists? false
           directory (fs/temp-dir "rocks-db")}
      :as options}]
  (let [handle (io/file directory)]
    (->RocksDB
     (.open JniDBFactory/factory
            handle
            (let [opts (Options.)]
              (doseq [[k v] options]
                (when (and v (contains? option-setters k))
                  ((option-setters k) opts v)))
              opts))
     (ReadOptions.)
     (WriteOptions.)
     handle)))
