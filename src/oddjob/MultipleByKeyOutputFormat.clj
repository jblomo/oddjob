(ns oddjob.MultipleByKeyOutputFormat
  (:import [org.apache.hadoop.fs Path]
           [java.util StringTokenizer])
  ; static class oddjob.MultipleByKeyOutputFormat extends MultipleTextOutputFormat<Text, Text> {
  (:gen-class :extends org.apache.hadoop.mapred.lib.MultipleTextOutputFormat))

(defn -generateFileNameForKeyValue
  "Generate the file output file name based on the given key and the leaf file
  name.

  This function joins the partial-path with the leaf file (typically part-0000x).
  Non-word characters are replaced with an underscore in the path."
  [this akey value leaf]
  (str (Path. (.replaceAll (.matcher #"[\W]" (str akey) ) "_") leaf)))
