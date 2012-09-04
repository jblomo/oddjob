(ns oddjob.MultipleValueJSONOutputFormat
  "Writes to the directories specified by the first element in the key.

  The output key of your job must be a JSON formatted array.  The first element
  will be used as the subdirectory, and the second element will be used for key
  written to the file."
  (:use [clojure.data.json :only (json-str read-json)])
  (:import [org.apache.hadoop.fs Path])
  ; static class oddjob.MultipleJSONOutputFormat extends MultipleTextOutputFormat<Text, Text> {
  (:gen-class :extends org.apache.hadoop.mapred.lib.MultipleTextOutputFormat))

(defn -generateFileNameForKeyValue
  "Generate the file output file name based on the given key and the leaf file
  name. akey is a JSON string of two elements:
  [partial-path, actual-key]
  This function parses the JSON, joins the partial-path with the leaf file
  (typically part-0000x).  Developer is responsible for ensuring the resulting
  path still passes FSNamesystem.isValidName."
  [this akey value leaf]
  (str (Path. (-> akey str read-json first str) leaf)))

(defn -generateActualKey
  "Generate the actual key from the given key/value. akey is a Key row, the
  first element of which is the partial-path.

  This function parses the Key and removes the parital-path."
  [this akey value]
  nil)