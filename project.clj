(defproject org.flausenhaus/clj-rocksdb "0.1.0-SNAPSHOT"

  :description "Clojure bindings for RocksDB, an embeddable persistent key-value store for fast storage extending LevelDB."

  :url "https://github.com/flausenhaus/clj-rocksdb"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[byte-streams "0.1.9"]
                 [com.taoensso/nippy "2.6.0-beta2"]
                 [me.raynes/fs "1.4.5"]
                 [org.clojure/core.cache "0.6.3"]
                 [org.clojure/core.memoize "0.5.6"]
                 [org.fusesource.rocksdbjni/rocksdbjni-osx "99-master-SNAPSHOT"]
                 [org.iq80.leveldb/leveldb-api "0.6"]
                 [org.clojure/clojure "1.5.1"]]

  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[criterium "0.4.3"]
                                  [codox-md "0.2.0"
                                   :exclusions [org.clojure/clojure]]
                                  [org.clojure/tools.namespace "0.2.4"]
                                  [org.clojure/java.classpath "0.2.0"]]}}

  :plugins [[lein-ancient "0.5.4"]
            [codox "0.6.4"]]

  :codox {:writer codox-md.writer/write-docs
          :include [clj-rocksdb]}

  :jvm-opts ^:replace ["-server"]

  :min-lein-version "2.0.0"

  :javac-options ["-target" "1.7" "-source" "1.7" "-Xlint:-options"]

  :test-selectors {:default (fn [x] (not (:benchmark x)))
                   :all (fn [_] true)
                   :benchmark :benchmark})
