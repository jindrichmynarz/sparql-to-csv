(ns sparql-to-csv.sparql-test
  (:require [sparql-to-csv.sparql :as sparql]
            [sparql-to-csv.test-helpers :refer [slurp-resource]]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))

(deftest sparql-update?
  (testing "SPARQL Update detection"
    (are [file update?] (is (= (sparql/sparql-update? (slurp-resource file)) update?))
         "invalid_query_1.rq" false
         "update.ru" true)))

(deftest validate-sparql
  (testing "SPARQL syntax validation"
    (are [file] (is (thrown? Exception (sparql/validate-sparql (slurp-resource file))))
         "invalid_query_1.rq"
         "invalid_query_2.rq")
    (is (sparql/validate-sparql (slurp-resource "update.ru") :update? true))))
