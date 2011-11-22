(ns oddjob.test.MultipleValueOutputFormat
  (:use [clojure.test]
        [clojure.string :only (join)])
  (:import [org.apache.hadoop.io Text]))

(defn- make-element
  "Encodes a clojure elements as a Key string in a hadoop Text class"
  [elem]
  (Text. (str elem)))

(def leaf "part-00000")

(def splitter (oddjob.MultipleValueOutputFormat.))

(def test-keys [["myfilename","rest"]
                ["myfilename",28342]
                [902342,"rest"]
                [2983042,493120]
                ["file,with,comma","rest"]
                ["file,with,comma",28342]
                ["file,with,comma",493120]])

(deftest gen-file-name
         (doseq [[akey value] test-keys]
           (let [path (.generateFileNameForKeyValue splitter (make-element akey) (make-element value) leaf)
                 correct (str akey "/" leaf)]
             (is (= path correct)))))

(deftest gen-actual-key
         (doseq [[akey value]test-keys]
           (let [output (.generateActualKey splitter (make-element akey) (make-element value))]
             (is (= output nil)))))
