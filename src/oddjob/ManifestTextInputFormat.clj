(ns oddjob.ManifestTextInputFormat
  (:import [org.apache.hadoop.fs Path]
           [org.apache.hadoop.mapred TextInputFormat]
           [java.io InputStreamReader BufferedReader])
  (:gen-class :extends org.apache.hadoop.mapred.TextInputFormat))

(defn- manifest->paths
  "Given a manifest, return a list paths in it"
  [manifest conf]
  (let [fs (.getFileSystem manifest conf)]
    (map #(Path. %) (->> manifest
                      (.open fs)
                      InputStreamReader.
                      BufferedReader.
                      line-seq))))

(defn- path->file-statuses
  "Given a Path return the all matching FileStatuses"
  [path conf]
  (let [fs (.getFileSystem path conf)]
    (.globStatus fs path)))

(defn -listStatus
  "Takes the nominal job input path as a manifest file and returns all the paths
  within the file."
  [this job]
  (let [manifests (TextInputFormat/getInputPaths job)
        paths (mapcat #(manifest->paths % job) manifests)
        file-statuses (mapcat #(path->file-statuses % job) paths)]
    (into-array file-statuses)))

