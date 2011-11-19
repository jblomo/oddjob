(ns oddjob.MultipleTextOutputFormatByKey
  (:import [org.apache.hadoop.fs Path]
           [java.util StringTokenizer])
  ; static class oddjob.MultipleTextOutputFormatByKey extends MultipleTextOutputFormat<Text, Text> {
  (:gen-class :extends org.apache.hadoop.mapred.lib.MultipleTextOutputFormat))

(defn -generateFileNameForKeyValue
  "Generate the file output file name based on the given key and the leaf file
  name.

  This function joins the partial-path with the leaf file (typically part-0000x).
  Developer is responsible for ensuring the resulting path still passes 
  FSNamesystem.isValidName."
  [this akey value leaf]
  (str (Path. (str akey) leaf)))
