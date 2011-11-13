(ns oddjob.MultipleCSVOutputFormat
  (:use [clojure.data.csv :only (read-csv write-csv)])
  (:import [org.apache.hadoop.fs Path]
           [java.util StringTokenizer])
  ; static class oddjob.MultipleCSVOutputFormat extends MultipleTextOutputFormat<Text, Text> {
  (:gen-class :extends org.apache.hadoop.mapred.lib.MultipleTextOutputFormat))

(defn -generateFileNameForKeyValue
  "Generate the file output file name based on the given key and the leaf file
  name. akey is a CSV string, the first element of which is the partial-path.

  This function reads the CSV, joins the partial-path with the leaf file
  (typically part-0000x).  Developer is responsible for ensuring the resulting
  path still passes FSNamesystem.isValidName."
  [this akey value leaf]
  (str (Path. (-> akey str read-csv ffirst str) leaf)))

(defn -generateActualKey
  "Generate the actual key from the given key/value. akey is a CSV row, the
  first element of which is the partial-path.

  This function reads the CSV and removes the parital-path.  Note: optional
  quotes in the original key may be removed if they are not required."
  [this akey value]
  (let [elements (first (read-csv (str akey)))
        swriter (doto (java.io.StringWriter.) (write-csv [(rest elements)]))]
    ; remove filename + comma
    (doto akey (.set (.trim (str swriter))))))
