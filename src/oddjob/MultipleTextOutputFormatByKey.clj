(ns oddjob.MultipleTextOutputFormatByKey
  "Writes to the directories specified by the key.

  The key of your job output will be used as the file path.  Both the key and
  the value will be written to the resulting tab delimited text files."
 (:import [org.apache.hadoop.fs Path])
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
