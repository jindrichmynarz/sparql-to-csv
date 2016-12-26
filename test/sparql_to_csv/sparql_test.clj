(ns sparql-to-csv.sparql-test
  (:require [sparql-to-csv.sparql :as sparql]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))

(deftest invalid-query?
  (testing "SPARQL syntax validation"
    (are [query-file] (is (#'sparql/validate-query (slurp (io/resource query-file))))
         "invalid_query_1.rq"
         "invalid_query_2.rq")
    (is (-> "invalid_query_2.rq"
            io/resource
            slurp
            (#'sparql/validate-query)
            (#'sparql/format-line-and-column)
            not)
        "No line and column information for violations of grouping keys.")))
