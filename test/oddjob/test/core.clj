(ns oddjob.test.core
  (:use [oddjob.core])
  (:use [clojure.test]))

(deftest main-implemented
  (is (fn? -main) "oddjob.core does not implement -main"))
