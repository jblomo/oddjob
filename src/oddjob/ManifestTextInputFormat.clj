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

(defn- expand-path
  "Expand a path to the contents if a directory, or matches if a glob, or just the given path."
  [path conf]
  (let [fs (.getFileSystem path conf)
        matches (.globStatus fs path)]
    (map #(.getPath %) (mapcat #(if (.isDir %)
                                  (.listStatus fs (.getPath %))
                                  [%]) matches))))

(defn- path->status
  "Given a Path return all the expanded matching FileStatuses."
  [path conf]
  (let [fs (.getFileSystem path conf)]
    (mapcat #(.globStatus fs %) (expand-path path conf))))

(defn -listStatus
  "Takes the nominal job input path as a manifest file and returns all the paths
  within the file."
  [this job]
  (let [manifests (mapcat #(expand-path % job) (TextInputFormat/getInputPaths job))
        paths (mapcat #(manifest->paths % job) manifests)
        file-statuses (mapcat #(path->status % job) paths)]
    (into-array file-statuses)))

