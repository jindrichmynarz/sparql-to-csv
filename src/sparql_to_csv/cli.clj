(ns sparql-to-csv.cli
  (:gen-class)
  (:require [sparql-to-csv.sparql :as sparql]
            [sparql-to-csv.endpoint :as endpoint]
            [sparql-to-csv.util :as util]
            [sparql-to-csv.mustache :as mustache]
            [sparql-to-csv.spec :as spec]
            [clojure.spec :as s]
            [clojure.tools.cli :refer [parse-opts]]
            [mount.core :as mount]
            [clojure.java.io :as io]
            [slingshot.slingshot :refer [try+]])
  (:import (java.io File Reader)))

; ----- Private functions -----

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
  [params]
  (when-not (s/valid? ::spec/config params)
    (util/die (str "The provided arguments are invalid.\n\n"
                   (s/explain-str ::spec/config params)))))

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
  [{:keys [::spec/endpoint ::spec/input]
    :as params}
   template]
  (validate-params params)
  (when-not (util/file-exists? (io/as-file template))
    (util/die (format "File at %s doesn't exist." template)))
  (let [template' (slurp template)
        piped? (has-input? input)
        query-fn (if piped? sparql/piped-query sparql/paged-query)]
    (validate-template template')
    (when-not (or piped? (mustache/is-paged? template'))
      (util/die "The provided template is missing LIMIT/OFFSET for paging."))
    (try+ (mount/start-with-args params)
          (catch [:type ::util/endpoint-not-found] _
            (util/die (format "SPARQL endpoint <%s> was not found." endpoint))))
    (try+ (query-fn params template')
          ; Virtuoso-specific spice
          (catch [:type ::util/incomplete-results] _
            (util/die "The endpoint returned incomplete results. Try lowering the page size."))
          (catch [:type ::util/not-found] _
            (util/die (format "SPARQL endpoint <%s> is hiding." endpoint)))
          (catch [:status 500] {:keys [body]}
            (util/die body)))))

; ----- Private vars -----

(def ^:private cli-options
  [["-e" "--endpoint ENDPOINT" "SPARQL endpoint's URL"
    :id ::spec/endpoint
    :validate [(every-pred spec/http? spec/valid-url?)
               "The endpoint must be a valid absolute HTTP(S) URL."]]
   ["-o" "--output OUTPUT" "Path to the output file"
    :id ::spec/output
    :parse-fn io/as-file
    :default *out*
    :default-desc "STDIN"]
   ["-i" "--input INPUT" "Path to the input file"
    :id ::spec/input
    :validate [util/file-exists? "The input file does not exist."]
    :parse-fn io/as-file
    :default *in*
    :default-desc "STDOUT"]
   ["-p" "--page-size PAGE_SIZE" "Number of results to fetch in one request"
    :id ::spec/page-size
    :parse-fn util/->integer
    :validate [pos? "Number of results must be a positive number."]
    :default 10000]
   [nil "--extend" "Extend piped CSV input with the new results instead of replacing it."
    :id ::spec/extend?
    :default false]
   ["-d" "--delimiter DELIMITER" "Character to delimit cells in the output"
    :id ::spec/delimiter
    :default \,]
   [nil "--sleep SLEEP" "Number of miliseconds to pause between requests"
    :id ::spec/sleep
    :parse-fn util/->integer
    :validate [spec/non-negative? "Pause duration must be a non-negative number."]
    :default 0]
   [nil "--start-from START_FROM" "Starting offset to skip initial results"
    :id ::spec/start-from
    :parse-fn util/->integer
    :validate [spec/non-negative? "Starting offset must be a non-negative number."]
    :default 0]
   [nil "--max-retries MAX_RETRIES" "Number of attempts to retry a failed request"
    :id ::spec/max-retries
    :parse-fn util/->integer
    :validate [spec/non-negative? "Number of retries must be a non-negative number."]
    :default 3]
   [nil "--parallel" "Execute queries in parallel"
    :id ::spec/parallel?
    :default false]
   ["-h" "--help" "Display help information"
    :id ::spec/help?]])

; ----- Public functions -----

(defn -main
  [& args]
  (let [{{:keys [::spec/help ::spec/endpoint]
          :as params} :options
         :keys [arguments errors summary]} (parse-opts args cli-options)
        [template] (filter (partial not= "-") arguments)]
    (cond help (util/info (usage summary))
          errors (util/die (error-msg errors))
          (not endpoint) (util/die "You must provide a SPARQL endpoint URL.")
          (not template) (util/die "You must provide a query template.")
          :else (main params template))))
