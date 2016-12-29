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
            [slingshot.slingshot :refer [try+]]))

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

(defn- main
  [{::spec/keys [endpoint piped?]
    :as params}
   template]
  (validate-params params)
  (validate-template template)
  (let [query-fn (cond piped? sparql/piped-query
                       (mustache/is-paged? template) sparql/paged-query
                       :else sparql/query)]
    (try+ (mount/start-with-args params)
          (catch [:type ::util/endpoint-not-found] _
            (util/die (format "SPARQL endpoint <%s> was not found." endpoint))))
    (try+ (query-fn params template)
          ; Virtuoso-specific spice
          (catch [:type ::util/incomplete-results] _
            (util/die "The endpoint returned incomplete results. Try lowering the page size."))
          (catch [:type ::util/not-found] _
            (util/die (format "SPARQL endpoint <%s> is hiding." endpoint)))
          (catch [:type ::util/invalid-column-names] {:keys [message]}
            (util/die message))
          (catch [:type ::util/invalid-query] {:keys [message]}
            (util/die message))
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
   [nil "--piped" "Use piped query parameters"
    :id ::spec/piped?
    :default false]
   [nil "--extend" "Extend piped CSV input with fetched columns" 
    :id ::spec/extend?
    :default false]
   [nil "--input-delimiter INPUT_DELIMITER" "Delimiter used in the input CSV"
    :id ::spec/input-delimiter
    :default \,]
   [nil "--output-delimiter OUTPUT_DELIMITER" "Delimiter used in the output CSV"
    :id ::spec/output-delimiter
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
   [nil "--skip-sparql-validation" "Skip validation of SPARQL syntax"
    :id ::spec/skip-sparql-validation?
    :default false]
   ["-h" "--help" "Display help information"
    :id ::spec/help?]])

; ----- Public functions -----

(defn -main
  [& args]
  (let [{{::spec/keys [help? endpoint]
          :as params} :options
         :keys [arguments errors summary]} (parse-opts args cli-options)
        [template] (filter (partial not= "-") arguments)]
    (cond help? (util/info (usage summary))
          errors (util/die (error-msg errors))
          (not endpoint) (util/die "You must provide a SPARQL endpoint URL.")
          (not template) (util/die "You must provide a query template.")
          (not (util/file-exists? (io/as-file template))) (util/die (format "File at %s doesn't exist." template))
          :else (main params (slurp template)))))
