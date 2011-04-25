(ns oddjob.test.MultipleCSVOutputFormat
  (:use [clojure.test]
        [clojure.string :only (join)])
  (:require [clojure.contrib.json :as json])
  (:import [org.apache.hadoop.io Text]))

(defn- make-element
  "Encodes a seqable as a CSV in a hadoop Text class"
  [aseq]
  (Text. (join "," (map json/json-str aseq))))

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
             (is (= path correct)))))

(deftest gen-actual-key
         (doseq [akey test-rows]
           (let [output (.generateActualKey splitter (make-element akey) value)
                 correct (make-element (rest akey))]
             (is (= output correct)))))
