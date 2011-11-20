(ns oddjob.ManifestTextInputFormat
  (:import [org.apache.hadoop.fs Path]
           [org.apache.hadoop.mapred TextInputFormat]
           [java.io InputStreamReader BufferedReader])
  (:gen-class :extends org.apache.hadoop.mapred.TextInputFormat))

(defn- manifest->paths
  "Given a manifest Path, return a list paths in it"
  [manifest conf]
  (let [fs (.getFileSystem manifest conf)]
    (map #(Path. %) (->> manifest
                      (.open fs)
                      InputStreamReader.
                      BufferedReader.
                      line-seq))))

(defn- expand-path
  "Expand a path to FileStatuses of:
  - the contents if a directory
  - matches if a glob
  - just the given path, if a file."
  [path conf]
  (let [fs (.getFileSystem path conf)
        matches (.globStatus fs path)]
    (mapcat #(if (.isDir %)
               (.listStatus fs (.getPath %))
               [%]) matches)))

(defn -listStatus
  "Takes the nominal job input path as a manifest file and returns all the paths
  within the file."
  [this job]
  (let [manifests (mapcat #(expand-path % job) (TextInputFormat/getInputPaths job))
        paths (mapcat #(manifest->paths (.getPath %) job) manifests)
        file-statuses (apply concat (pmap #(expand-path % job) paths))]
    (into-array file-statuses)))

