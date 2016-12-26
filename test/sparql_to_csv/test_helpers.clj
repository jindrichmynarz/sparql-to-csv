(ns sparql-to-csv.test-helpers
  (:require [clojure.java.io :as io]))

(def slurp-resource
  (comp slurp io/resource))
