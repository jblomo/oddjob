(ns oddjob.MultipleCSVOutputFormat
  (:require [clojure.contrib.json :as json])
  (:import [org.apache.hadoop.fs Path]
           [java.util StringTokenizer])
  ; static class oddjob.MultipleCSVOutputFormat extends MultipleTextOutputFormat<Text, Text> {
  (:gen-class :extends org.apache.hadoop.mapred.lib.MultipleTextOutputFormat))

(defn- first-key
  "Utility function to grab the first element in a CSV row, even if that element
  has a delimiter in a quoted string."
  [row]
  (let [[akey & xs] (.split row ",")]
    (if (= (first row) \")
      (apply str akey (take-while #(not= (last %) \") xs))
      akey)))

(defn -generateFileNameForKeyValue
  "Generate the file output file name based on the given key and the leaf file
  name. akey is a CSV string, the first element of which is the partial-path.

  This function parses the CSV, joins the partial-path with the leaf file
  (typically part-0000x).  Developer is responsible for ensuring the resulting
  path still passes FSNamesystem.isValidName."
  [this akey value part]
  (str (Path. (-> akey str first-key json/read-json str) part)))

(defn -generateActualKey
  "Generate the actual key from the given key/value. akey is a CSV row, the
  first element of which is the partial-path.

  This function parses the CSV and removes the parital-path."
  [this akey value]
  (let [key-string (str akey)
        partial-path (first-key key-string)]
    ; set key minus filename + comma
    (doto akey (.set (.substring key-string (inc (count partial-path)))))))

(defn -main
  [& args]
  (org.apache.hadoop.streaming.HadoopStreaming/main (into-array String args)))
