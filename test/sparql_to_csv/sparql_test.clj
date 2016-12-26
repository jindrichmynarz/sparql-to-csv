(ns sparql-to-csv.sparql-test
  (:require [sparql-to-csv.sparql :as sparql]
            [sparql-to-csv.test-helpers :refer [slurp-resource]]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))

(deftest invalid-query?
  (testing "SPARQL syntax validation"
    (are [query-file] (is (#'sparql/validate-query (slurp-resource query-file)))
         "invalid_query_1.rq"
         "invalid_query_2.rq")
    (is (-> "invalid_query_2.rq"
            slurp-resource
            (#'sparql/validate-query)
            (#'sparql/format-line-and-column)
            not)
        "No line and column information for violations of grouping keys.")))
