(ns sparql-to-csv.mustache-test
  (:require [sparql-to-csv.mustache :as mustache]
            [sparql-to-csv.test-helpers :refer [slurp-resource]]
            [stencil.parser :refer [valid-tag-content]]
            [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck.generators :as gen']
            [clojure.java.io :as io]))

(deftest validate-template
  (testing "Mustache syntax validation"
    (is (mustache/validate (slurp-resource "valid_template.mustache")))
    (is (thrown? Exception (mustache/validate (slurp-resource "invalid_template.mustache"))))))

(deftest is-paged?
  (is (mustache/is-paged? (slurp-resource "valid_template.mustache")))
  (is (not (mustache/is-paged? (slurp-resource "query_template.mustache")))))

(deftest get-template-variables
  (is (= #{:contractingAuthority :supplier}
         (mustache/get-template-variables (slurp-resource "query_template.mustache")))))

(defspec valid-variable-name?
  100
  (prop/for-all [variable-name (gen'/string-from-regex valid-tag-content)]
                (mustache/valid-variable-name? variable-name)))
