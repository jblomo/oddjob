(ns oddjob.core
  (:gen-class))

(defn -main
  [& args]
  (org.apache.hadoop.streaming.HadoopStreaming/main (into-array String args)))
