(ns oddjob.test.MultipleTextOutputFormatByKey
  (:use [clojure.test])
  (:import [org.apache.hadoop.io Text]))

(defn- make-element
  "Encodes a clojure elements as a Key string in a hadoop Text class"
  [elem]
  (Text. (str elem)))

(def leaf "part-00000")

(def splitter (oddjob.MultipleTextOutputFormatByKey.))

(def test-rows [["myfilename","somejunkvalue"],
                [902342,2234234],
                ["file,with,comma","value,with,comma"],
                ["file.with.dot","value.with.dot"],
                ["file/with/forward/slash","value/with/forward/slash"],
                ["filewith,every/thing.2","valuewith,every/thing.2"]])

(deftest gen-file-name
         (doseq [[testkey testvalue] test-rows]
           (let [path (.generateFileNameForKeyValue splitter (make-element testkey) (make-element testvalue) leaf)
                 correct (str testkey "/" leaf)]
             (is (= path correct)))))

(deftest gen-actual-key
         (doseq [[testkey testvalue] test-rows]
           (let [output (.generateActualKey splitter (make-element testkey) testvalue)
                 correct (make-element testkey)]
             (is (= output correct)))))
