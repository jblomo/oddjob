(defproject oddjob "0.20.0-SNAPSHOT"
            :description "Hadoop utilities for MrJob that must run in the JVM"
            :dependencies [[org.clojure/clojure "1.2.1"]
                           [org.clojure/clojure-contrib "1.2.0"]]
            :dev-dependencies [[org.apache.hadoop/hadoop-streaming "0.20.2"]
                               #_[org.apache.hadoop/hadoop-core-with-dependencies "0.20.2"]
                               [swank-clojure "1.2.1"]]
            :aot :all)
