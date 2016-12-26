(ns sparql-to-csv.mustache
  (:require [sparql-to-csv.util :as util]
            [stencil.parser :refer [parse valid-tag-content]]
            [slingshot.slingshot :refer [throw+]]
            [clojure.set :refer [subset?]]))

(defn get-template-variables
  "Get a set of variables used in `template`."
  [template]
  (->> (parse template)
       (filter (complement string?))
       (mapcat :name)
       set))

(defn is-paged?
  "Is the query in `template` paged?"
  [template]
  (subset? #{:limit :offset} (get-template-variables template)))

(defn valid-variable-name?
  "Test if `variable-name` has a valid Mustache syntax."
  [variable-name]
  (seq (re-matches valid-tag-content variable-name)))

(defn validate
  "Validate the syntax of Mustache in `template`."
  [template]
  (try (parse template)
       (catch Exception e
         (throw+ {:type ::util/invalid-mustache-syntax
                  :message (util/join-lines ["The provided Mustache template is invalid:"
                                             (.getMessage e)])}))))
