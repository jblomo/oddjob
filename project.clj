(defproject oddjob "1.0.1"
  :description "Hadoop utilities for MrJob that must run in the JVM"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.json "0.2.2"]
                 [org.clojure/data.csv "0.1.2"]]
  :license {:name "Apache License, Version 2.0 "
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :profiles {:dev {:resource-paths ["test-resources"]
                   :dependencies [[org.apache.hadoop/hadoop-streaming "0.20.2"]]}}
  :aot :all)
