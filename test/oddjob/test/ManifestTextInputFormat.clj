(ns oddjob.test.ManifestTextInputFormat
  (:use [clojure.test]
        [clojure.string :only (join)]
        [clojure.java.io :only (resource)])
  (:import [oddjob ManifestTextInputFormat]
           [org.apache.hadoop.mapreduce JobID]
           [org.apache.hadoop.mapreduce JobContext]
           [org.apache.hadoop.conf Configuration]))

(def jobid (JobID. "test" 1))
(def input-format (ManifestTextInputFormat.))
(def conf (Configuration.))
(.set conf "mapred.input.dir" (str (resource "manifest.txt")))

(def job-context (JobContext. conf jobid))

(defn- status->filename
  "Given a FileStatus, return the filename without the path"
  [status]
  (-> status .getPath str (.split "/") last))

(deftest find-all-files
  (let [file-statuses (.listStatus input-format job-context)
        filenames (set (map status->filename file-statuses))]
    (is (= filenames #{"file-a.txt" "file-b.txt"}))))




