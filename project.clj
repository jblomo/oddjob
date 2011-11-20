(defproject oddjob "0.38.0-SNAPSHOT"
            :description "Hadoop utilities for MrJob that must run in the JVM"
            :dependencies [[org.clojure/clojure "1.3.0"]
                           [org.clojure/data.json "0.1.2"]
                           [org.clojure/data.csv "0.1.0"]]
            :dev-dependencies [[org.apache.hadoop/hadoop-streaming "0.20.2"]
                               #_[org.apache.hadoop/hadoop-core-with-dependencies "0.20.2"]]
            :aot :all)
