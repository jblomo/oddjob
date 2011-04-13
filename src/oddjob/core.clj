(ns oddjob.core
  (:require [clojure.contrib.json :as json])
  (:import [org.apache.hadoop.fs Path]
           [org.apache.hadoop.mapred.lib MultipleTextOutputFormat])
  (:gen-class :extends org.apache.hadoop.mapred.lib.MultipleTextOutputFormat)
  )

; static class MultiFileOutput extends MultipleTextOutputFormat<Text, Text> {
(defn -generateActualKey [this akey value]
  (doto akey (.set (-> akey str json/read-json second json/json-str))))

(defn -generateFileNameForKeyValue [this akey value aname]
  (str (Path. (first (json/read-json (str akey))) aname)))

(defn -main [& args]
  (org.apache.hadoop.streaming.HadoopStreaming/main (into-array String args)))
