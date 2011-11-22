(ns oddjob.MultipleValueOutputFormat
  "Writes to the directories specified by the key, and only writes the value

  The key of your job output will be used as the file path.  Only the value will
  be written to the resulting files."
  (:import [org.apache.hadoop.fs Path])
  ; static class oddjob.MultipleValueOutputFormat extends MultipleTextOutputFormat<Text, Text> {
  (:gen-class :extends org.apache.hadoop.mapred.lib.MultipleTextOutputFormat))

(defn -generateFileNameForKeyValue
  "Generate the file output file name based on the given key and the leaf file
  name. akey is a string which is the partial-path.

  This function parses the Key, joins the partial-path with the leaf file
  (typically part-0000x).  Developer is responsible for ensuring the resulting
  path still passes FSNamesystem.isValidName."
  [this akey value leaf]
  (str (Path. (str akey) leaf)))

(defn -generateActualKey
  "Generate the actual key from the given key/value. akey is a Key row, the
  first element of which is the partial-path.

  This function parses the Key and removes the parital-path."
  [this akey value]
  nil)
