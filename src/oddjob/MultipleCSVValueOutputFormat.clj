(ns oddjob.MultipleCSVValueOutputFormat
  (:import [org.apache.hadoop.fs Path]
           [java.util StringTokenizer])
  ; static class oddjob.MultipleCSVValueOutputFormat extends MultipleTextOutputFormat<Text, Text> {
  (:gen-class :extends org.apache.hadoop.mapred.lib.MultipleTextOutputFormat))

(defn -generateFileNameForKeyValue
  "Generate the file output file name based on the given key and the leaf file
  name. akey is the partial-path, value is csv string.

  This function parses the CSV, joins the partial-path with the leaf file
  (typically part-0000x).  Developer is responsible for ensuring the resulting
  path still passes FSNamesystem.isValidName."
  [this akey value leaf]
  (str (Path. akey leaf)))

(defn -generateActualKey
  "Generate the actual key to output value only."
  [this akey value]
  nil)
