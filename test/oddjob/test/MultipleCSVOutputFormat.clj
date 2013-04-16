(ns oddjob.test.MultipleCSVOutputFormat
  (:use [clojure.test]
        [clojure.string :only (join)]
        [clojure.data.csv :only (read-csv write-csv)]
        [clojure.data.json :only (write-str)])
  (:import [org.apache.hadoop.io Text]))

(defn- make-element
  "Encodes a seqable as a CSV in a hadoop Text class.  Write a customized string
  to avoid dependencies on csv library."
  [aseq]
  (Text. (join "," (map write-str aseq))))

(defn- text-to-vec
  "Converts a hadoop.io.Text object to a parsed vector."
  [text]
  (first (read-csv (str text))))

(def value (Text.))

(def leaf "part-00000")

(def splitter (oddjob.MultipleCSVOutputFormat.))

(def test-rows [["myfilename","rest","of","csv",2]
                ["myfilename",28342,"rest","of","csv"]
                [902342,"rest","of","csv",2]
                [2983042,493120,1398541230,91304]
                ["file,with,comma","rest","of","csv",2]
                ["file,with,comma",28342,"rest","of","csv"]
                ["file,with,comma",493120,1398541230,91304]])

(deftest gen-file-name
         (doseq [akey test-rows]
           (let [path (.generateFileNameForKeyValue splitter (make-element akey) value leaf)
                 correct (str (first akey) "/" leaf)]
             (is (apply = (map text-to-vec [path correct]))))))

(deftest gen-actual-key
         (doseq [akey test-rows]
           (let [output (.generateActualKey splitter (make-element akey) value)
                 correct (make-element (rest akey))]
             (is (apply = (map text-to-vec [output correct]))))))
