(ns sparql-to-csv.cli
  (:gen-class)
  (:require [sparql-to-csv.sparql :as sparql]
            [sparql-to-csv.endpoint :as endpoint]
            [sparql-to-csv.util :as util]
            [sparql-to-csv.mustache :as mustache]
            [clojure.tools.cli :refer [parse-opts]]
            [mount.core :as mount]
            [clojure.java.io :as io]
            [slingshot.slingshot :refer [try+]])
  (:import (java.io File Reader)))

; ----- Private functions -----

(def ^:private non-negative?
  (complement neg?))

(defn- usage
  [summary]
  (util/join-lines ["Stream SPARQL results to CSV."
                    ""
                    "Usage: sparql_to_csv [options] query"
                    ""
                    "Options:"
                    summary]))

(defn- error-msg
  [errors]
  (util/join-lines (cons "The following errors occurred while parsing your command:\n" errors)))

(defn- validate-template
  [template]
  (try+ (mustache/validate template)
        (catch [:type ::util/invalid-mustache-syntax] {:keys [message]}
          (util/die message))))

(defn- validate-params
  [{:keys [query]}]
  ; TODO: clojure.spec validation
  )

(defmulti has-input?
  "Test if `input` provides something."
  type)

(defmethod has-input? File
  [input]
  (with-open [reader (io/reader input)]
    (.ready reader)))

(defmethod has-input? Reader
  [input]
  (let [c (.read input)]
    (.unread input c)
    (boolean c)))

(defn- main
  [{:keys [endpoint input]
    :as params}
   template]
  (validate-params params)
  (validate-template template)
  (let [piped? (has-input? input)
        query-fn (if piped? sparql/piped-query sparql/paged-query)]
    (when-not (or piped? (mustache/is-paged? template))
      (util/die "The provided template is missing LIMIT/OFFSET for paging."))
    (try+ (mount/start-with-args params)
          (catch [:type ::util/endpoint-not-found] _
            (util/die (format "SPARQL endpoint <%s> was not found." endpoint))))
    (try+ (query-fn params template)
          ; Virtuoso-specific spice
          (catch [:type ::util/incomplete-results] _
            (util/die "The endpoint returned incomplete results. Try lowering the page size."))
          (catch [:type ::util/not-found] _
            (util/die (format "SPARQL endpoint <%s> is hiding." endpoint)))
          (catch [:status 500] {:keys [body]}
            (util/die body)))))

; ----- Private vars -----

(def ^:private cli-options
  [["-e" "--endpoint ENDPOINT" "SPARQL endpoint's URL"]
   ["-o" "--output OUTPUT" "Path to the output file"
    :parse-fn io/as-file
    :default *out*
    :default-desc "STDIN"]
   ["-i" "--input INPUT" "Path to the input file"
    :validate [util/file-exists? "The input file does not exist."]
    :parse-fn io/as-file
    :default *in*
    :default-desc "STDOUT"]
   ["-p" "--page-size PAGE_SIZE" "Number of results to fetch in one request"
    :parse-fn util/->integer
    :validate [pos? "Number of results must be a positive number."]
    :default 10000]
   [nil "--extend" "Extend piped CSV input with the new results instead of replacing it."
    :id :extend?
    :default false]
   ["-d" "--delimiter DELIMITER" "Character to delimit cells in the output"
    :default \,]
   [nil "--sleep SLEEP" "Number of miliseconds to pause between requests"
    :parse-fn util/->integer
    :validate [non-negative? "Pause duration must be a non-negative number."]
    :default 0]
   [nil "--start-from START_FROM" "Starting offset to skip initial results"
    :parse-fn util/->integer
    :validate [non-negative? "Starting offset must be a non-negative number."]
    :default 0]
   [nil "--max-retries MAX_RETRIES" "Number of attempts to retry a failed request"
    :parse-fn util/->integer
    :validate [non-negative? "Number of retries must be a non-negative number."]
    :default 3]
   [nil "--parallel" "Execute queries in parallel"
    :id :parallel?
    :default false]
   ["-h" "--help" "Display help information"]])

; ----- Public functions -----

(defn -main
  [& args]
  (let [{{:keys [help]
          :as params} :options
         :keys [arguments errors summary]} (parse-opts args cli-options)
        [template] (filter (partial not= "-") arguments)]
    (cond help (util/info (usage summary))
          errors (util/die (error-msg errors))
          (not template) (util/die "You must provide a query template.")
          :else (main params (slurp template)))))
