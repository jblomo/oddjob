(ns oddjob.test.MultipleJSONOutputFormat
  (:use [clojure.test]
        [clojure.string :only (join)]
        [clojure.data.json :only (json-str)])
  (:import [org.apache.hadoop.io Text]))

(defn- make-element
  "Encodes a clojure elements as a JSON string in a hadoop Text class"
  [elem]
  (Text. (json-str elem)))

(def value (Text.))

(def leaf "part-00000")

(def splitter (oddjob.MultipleJSONOutputFormat.))

(def test-keys [["myfilename","rest"]
                ["myfilename",28342]
                [902342,"rest"]
                [2983042,493120]
                ["file,with,comma","rest"]
                ["file,with,comma",28342]
                ["file,with,comma",493120]])

(deftest gen-file-name
         (doseq [akey test-keys]
           (let [path (.generateFileNameForKeyValue splitter (make-element akey) value leaf)
                 correct (str (first akey) "/" leaf)]
             (is (= path correct)))))

(deftest gen-actual-key
         (doseq [akey test-keys]
           (let [output (.generateActualKey splitter (make-element akey) value)
                 correct (make-element (second akey))]
             (is (= output correct)))))
