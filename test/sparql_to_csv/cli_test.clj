(ns sparql-to-csv.cli-test
  (:require [sparql-to-csv.cli :as cli]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))

(deftest has-input?
  (is (not (cli/has-input? *in*)))
  (is (not (cli/has-input? (io/as-file (io/resource "empty.csv"))))))
