(ns sparql-to-csv.mustache
  (:require [sparql-to-csv.util :as util]
            [stencil.parser :refer [parse]]
            [slingshot.slingshot :refer [throw+]]
            [clojure.set :refer [subset?]]))

(defn get-template-variables
  "Get a set of variables used in `template`."
  [template]
  (->> (parse template)
       (filter (complement string?))
       (mapcat :name)
       distinct
       set))

(defn is-paged?
  "Is the query in `template` paged?"
  [template]
  (subset? #{:limit :offset} (get-template-variables template)))

(defn validate
  "Validate the syntax of Mustache in `template`."
  [template]
  (try (parse template)
       (catch Exception e
         (throw+ {:type ::util/invalid-mustache-syntax
                  :message (util/join-lines ["The provided Mustache template is invalid:"
                                             (.getMessage e)])}))))
