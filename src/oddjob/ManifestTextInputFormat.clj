(ns oddjob.ManifestTextInputFormat
  "Manifest files are inputs with a list of paths to use as the real input.

  Paths may be directories, globs, or files and will be expanded appropriately
  Unlike most InputFormats, this class will silently ignore missing and
  unmatched paths in the manifest file."
  (:import [org.apache.hadoop.fs Path]
           [org.apache.hadoop.mapred TextInputFormat]
           [java.io InputStreamReader BufferedReader]
           [org.apache.commons.logging LogFactory])
  (:gen-class :extends org.apache.hadoop.mapred.TextInputFormat
              :exposes-methods {listStatus superlistStatus}))

(defn- manifest->paths
  "Given a manifest Path, return a list of the paths it contains."
  [manifest conf]
  (let [fs (.getFileSystem manifest conf)]
    (with-open [data-stream (.open fs manifest)]
      ; don't be lazy, iterate through the whole file before it is closed
      (doall (map #(Path. %) (-> data-stream
                        InputStreamReader.
                        BufferedReader.
                        line-seq))))))

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
               [%])
            matches)))

(defn -listStatus
  "Takes the nominal job input paths as manifest files and returns all the FileStatus
  objects within those files."
  [this job]
  (let [manifests (. this superlistStatus job)
        paths (mapcat #(manifest->paths (.getPath %) job) manifests)
        ; expand the manifest paths in parallel and put them all in one list
        file-statuses (apply concat (pmap #(expand-path % job) paths))
        log (LogFactory/getLog (class this))]
    (.info log (str "Total input paths from manifest : " (count file-statuses)))
    (into-array file-statuses)))

