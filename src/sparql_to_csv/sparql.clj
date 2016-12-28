(ns sparql-to-csv.sparql
  (:require [sparql-to-csv.endpoint :refer [endpoint]]
            [sparql-to-csv.mustache :as mustache]
            [sparql-to-csv.util :as util]
            [sparql-to-csv.spec :as spec]
            [clj-http.client :as client]
            [stencil.core :refer [render-string]]
            [slingshot.slingshot :refer [throw+ try+]]
            [clojure.data.xml :as xml]
            [clojure.zip :as zip]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:import (org.apache.jena.query QueryParseException QueryFactory Syntax)))

; ----- Private functions -----

(defn- get-binding
  "Get binding for `variable` from `result`."
  [result variable]
  (zip-xml/xml1-> result :binding (zip-xml/attr= :name variable) zip-xml/text))

(defn sparql-results->clj
  "Convert SPARQL `results` into Clojure data structures.
  `header?` indicates if header should be output.
  `base-line` is a row (a vector) of values to which to append newly fetched columns.
  `base-header` is a header row for the `base-line`.
  Returns an empty sequence when the query has no results."
  [results & {:keys [base-line base-header header?]}]
  (let [zipper (-> results xml/parse-str zip/xml-zip)
        variables (vec (zip-xml/xml-> zipper :head :variable (zip-xml/attr :name)))
        results (zip-xml/xml-> zipper :results :result)]
    (reduce (fn [acc result]
              (conj acc (into base-line
                              (mapv (some-fn (partial get-binding result) (constantly "")) variables))))
            (if header? [(into base-header variables)] [])
            results)))

(defn execute-query
  "Execute SPARQL `query`." 
  [query & {:keys [retries]
            :or {retries 0}}]
  (let [{:keys [max-retries sparql-endpoint sleep virtuoso?]} endpoint
        params {:query-params {"query" query}
                :throw-entire-message? true}]
    (when-not (zero? sleep) (Thread/sleep sleep))
    (try+ (let [response (client/get sparql-endpoint params)]
            (if (and virtuoso? (= (get-in response [:headers "X-SQL-State"]) "S1TAT"))
              (throw+ {:type ::util/incomplete-results})
              (:body response)))
          (catch [:status 404] _
            (if (< retries max-retries)
              (do (Thread/sleep (+ (* retries 1000) 1000))
                  (execute-query query :retries (inc retries)))
              (throw+ {:type ::util/endpoint-not-found}))))))

(defn- validate-query
  "Validate SPARQL syntax of `query`."
  [query]
  (try (QueryFactory/create query Syntax/syntaxSPARQL_11) false
       (catch QueryParseException ex ex)))

(defn- format-query-parse-exception
  [query exception]
  (when exception
    (str "Syntax error in SPARQL query:\n\n"
         query
         "\n\n"
         (.getMessage exception))))

(defn invalid-query?
  "Test if SPARQL syntax of `query` is invalid."
  [query]
  (when-let [exception (validate-query query)]
    (format-query-parse-exception query exception)))

(defn csv-seq
  "Executes a query generated from SPARQL `template` for each item from `params`.
  Optionally extends input `lines` with the obtained SPARQL results."
  [{:keys [::spec/extend? ::spec/output ::spec/output-delimiter
           ::spec/parallel? ::spec/start-from]
    :as params}
   template
   param-seq
   stopping-condition
   & [lines & _]]
  (let [append? (pos? start-from)
        [base-header base-lines] (if extend?
                                   [(first lines) (drop (inc start-from) lines)]
                                   [[] (repeat [])])
        map-fn (if parallel? pmap map)
        query-fn (fn [param base-line index]
                   (let [start? (zero? index)
                         query (render-string template param)]
                     (when-let [error (and start? (invalid-query? query))]
                       (throw+ {:type ::util/invalid-query
                                :message error}))
                     (-> query
                         execute-query
                         (sparql-results->clj :header? (and start? (not append?))
                                              :base-line base-line
                                              :base-header base-header))))
        results (->> (map-fn query-fn param-seq base-lines (iterate inc 0))
                     (take-while stopping-condition)
                     util/lazy-cat')
        write-fn (fn [writer] (csv/write-csv writer results :delimiter output-delimiter))]
    ; Don't close the standard output
    (if (= output *out*)
      (write-fn output)
      (with-open [writer (io/writer output :append (and append? (util/file-exists? output)))]
        (write-fn writer)))))

(defn query
  "Run a SPARQL query from `query-string`."
  [params query-string]
  (csv-seq params query-string [{}] (constantly true)))

(defn paged-query
  "Run the query from `template` split into LIMIT/OFFSET delimited pages."
  [{:keys [::spec/page-size ::spec/start-from]
    :as params}
   template]
  (csv-seq params
           template
           (map (partial assoc {:limit page-size} :offset)
                (iterate (partial + page-size) start-from))
           seq))

(defn piped-query
  "Run the query from `template` with each line of input provided as template parameters."
  [{:keys [::spec/input ::spec/input-delimiter]
    :as params}
   template]
  (with-open [reader (io/reader input)]
    (let [lines (csv/read-csv reader :separator input-delimiter)
          head (first lines)
          header (map keyword head)]
      (mustache/validate-header-names head)
      (csv-seq params
               template
               (map (partial zipmap header) (next lines))
               (constantly true)
               lines))))
