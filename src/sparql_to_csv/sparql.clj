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
  (:import (org.apache.jena.query QueryParseException QueryFactory Syntax)
           (org.apache.jena.update UpdateFactory)))

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

(defn- prefix-virtuoso-operation
  "Prefix `sparql-string` for Virtuoso if `virtuoso?` is true."
  [^Boolean virtuoso?
   ^String sparql-string]
  (if virtuoso?
    (str "DEFINE sql:log-enable 2\n" sparql-string)
    sparql-string))

(defn execute-query
  "Execute SPARQL `query`." 
  [query & {:keys [retries update?]
            :or {retries 0}}]
  (let [{:keys [auth max-retries sparql-endpoint sleep virtuoso?]} endpoint
        [http-fn params-key] (if update? [client/get :query-params] [client/post :form-params])
        params (cond-> {params-key {"query" (prefix-virtuoso-operation virtuoso? query)}
                        :throw-entire-message? true}
                 auth (assoc :digest-auth auth))]
    (when-not (zero? sleep) (Thread/sleep sleep))
    (try+ (let [response (http-fn sparql-endpoint params)]
            (if (and virtuoso? (= (get-in response [:headers "X-SQL-State"]) "S1TAT"))
              (throw+ {:type ::util/incomplete-results})
              (:body response)))
          (catch [:status 404] _
            (if (< retries max-retries)
              (do (Thread/sleep (+ (* retries 1000) 1000))
                  (execute-query query :retries (inc retries)))
              (throw+ {:type ::util/endpoint-not-found}))))))

(def sparql-update?
  "A simple test of SPARQL Update operation"
  (comp boolean
        (some-fn (partial re-find #"(?i)(^|\s)\b(DELETE|INSERT)\s+(WHERE|DATA)?")
                 (partial re-find #"(?i)(^|\s)\b(LOAD|CLEAR|CREATE|DROP|COPY|MOVE|ADD)\s+"))))

(defn- format-parse-exception
  [sparql exception]
  (str "SPARQL syntax error:\n\n"
       sparql
       "\n\n"
       (.getMessage exception)))

(defn validate-query
  [sparql]
  (QueryFactory/create sparql Syntax/syntaxSPARQL_11))

(defn validate-update
  [sparql]
  (UpdateFactory/create sparql Syntax/syntaxSPARQL_11))

(defn validate-sparql
  "Validate syntax of `sparql`. Test as a SPARQL query by default.
  Test as a SPARQL Update operation if `update?`."
  [sparql & {:keys [update?]}]
  (let [validation-fn (if update? validate-update validate-query)]
    (try (validation-fn sparql)
         (catch QueryParseException exception
           (throw+ {:type ::util/invalid-sparql-syntax
                    :message (format-parse-exception sparql exception)})))))

(defn csv-seq
  "Executes a query generated from SPARQL `template` for each item from `params`.
  Optionally extends input `lines` with the obtained SPARQL results."
  [{::spec/keys [extend? output output-delimiter parallel?
                 skip-sparql-validation? start-from]
    :keys [sparql-syntax]
    :as params}
   template
   param-seq
   stopping-condition
   & [lines & _]]
  (let [append? (pos? start-from)
        update? (sparql-update? template)
        [base-header base-lines] (if extend?
                                   [(first lines) (drop (inc start-from) lines)]
                                   [[] (repeat [])])
        map-fn (if parallel? pmap map)
        query-fn (fn [param base-line index]
                   (let [start? (zero? index)
                         query (render-string template param)]
                     (and (not skip-sparql-validation?)
                          start?
                          (validate-sparql query :update? update?))
                     (when update? (println (format "Executing SPARQL update %d" (inc index))))
                     (cond-> (execute-query query :update? update?)
                             (not update?) (sparql-results->clj :header? (and start? (not append?))
                                                                :base-line base-line
                                                                :base-header base-header))))
        results (->> (map-fn query-fn param-seq base-lines (iterate inc 0))
                     (take-while stopping-condition)
                     util/lazy-cat')]
    (if update?
      (dorun results)
      (let [write-fn (fn [writer] (csv/write-csv writer results :delimiter output-delimiter))]
        ; Don't close the standard output
        (if (= output *out*)
          (do (write-fn output) (flush))
          (with-open [writer (io/writer output :append (and append? (util/file-exists? output)))]
            (write-fn writer)))))))

(defn query
  "Run a SPARQL query from `query-string`."
  [params query-string]
  (csv-seq params query-string [{}] (constantly true)))

(defn paged-query
  "Run the query from `template` split into LIMIT/OFFSET delimited pages."
  [{::spec/keys [page-size start-from]
    :as params}
   template]
  (csv-seq params
           template
           (map (partial assoc {:limit page-size} :offset)
                (iterate (partial + page-size) start-from))
           seq))

(defn piped-query
  "Run the query from `template` with each line of input provided as template parameters."
  ([{::spec/keys [input]
     :as params}
    template]
   (if (= input *in*)
     (piped-query params template (io/reader input)) ; Don't close standard input
     (with-open [reader (io/reader input)]
       (piped-query params template reader))))
  ([{::spec/keys [input-delimiter]
     :as params}
    template
    reader]
    (let [lines (csv/read-csv reader :separator input-delimiter)
          head (first lines)
          header (map keyword head)]
      (mustache/validate-header-names head)
      (csv-seq params
               template
               (map (partial zipmap header) (next lines))
               (constantly true)
               lines))))
