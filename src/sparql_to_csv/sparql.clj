(ns sparql-to-csv.sparql
  (:require [sparql-to-csv.endpoint :refer [endpoint]]
            [sparql-to-csv.mustache :as mustache]
            [sparql-to-csv.util :as util]
            [clj-http.client :as client]
            [stencil.core :refer [render-string]]
            [slingshot.slingshot :refer [throw+ try+]]
            [clojure.data.xml :as xml]
            [clojure.zip :as zip]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as string])
  (:import (org.apache.jena.query QueryParseException QueryFactory Syntax)))

; ----- Private functions -----

(defn- get-binding
  "Get binding for `variable` from `result`."
  [result variable]
  (zip-xml/xml1-> result :binding (zip-xml/attr= :name variable) zip-xml/text))

(defn sparql-results->clj
  "Convert SPARQL `results` into Clojure data structures.
  `header?` indicates if header should be output.
  Returns an empty sequence when the query has no results."
  [results & {:keys [header?]}]
  (let [zipper (-> results xml/parse-str zip/xml-zip)
        variables (vec (zip-xml/xml-> zipper :head :variable (zip-xml/attr :name)))
        results (zip-xml/xml-> zipper :results :result)]
    (reduce (fn [acc result]
              (conj acc (mapv (some-fn (partial get-binding result) (constantly "")) variables)))
            (if header? [variables] [])
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

(defn- format-line-and-column
  "Format line and column from `exception`."
  [exception]
  (let [line (.getLine exception)
        column (.getColumn exception)]
    (when-not (some (partial = -1) [line column])
      (format "\nLine: %d, column: %d" line column))))

(defn- format-query-parse-exception
  [query exception]
  (when exception
    (str "Syntax error in SPARQL query:\n\n"
         query
         "\n\n"
         (.getMessage exception)
         (format-line-and-column exception))))

(defn invalid-query?
  "Test if SPARQL syntax of `query` is invalid."
  [query]
  (when-let [exception (validate-query query)]
    (format-query-parse-exception query exception)))

(defn csv-seq
  [{:keys [delimiter extend? output parallel? start-from]
    :as params}
   template
   param-seq
   & [lines & _]]
  (let [append? (pos? start-from)
        map-fn (if parallel? pmap map)
        query-fn (fn [param index]
                   (let [start? (zero? index)
                         query (render-string template param)]
                     (when start?
                       (when-let [error (invalid-query? query)]
                         (util/die error)))
                     (-> query
                         execute-query
                         (sparql-results->clj :header? (and start? (not append?))))))]
    (with-open [writer (io/writer output :append (and append? (util/file-exists? output)))]
      (dorun (csv/write-csv writer
                            (cond->> (->> (map-fn query-fn param-seq (iterate inc 0))
                                          (take-while seq)
                                          util/lazy-cat')
                              extend? (map into (drop start-from lines)))
                            :separator delimiter)))))

(defn paged-query
  "Run the query from `template` split into LIMIT/OFFSET delimited pages."
  [{:keys [page-size start-from]
    :as params}
   template]
  (csv-seq params
           template
           (map (partial assoc {:limit page-size} :offset)
                (iterate (partial + page-size) start-from))))

(defn piped-query
  "Run the query from `template` with each line of input provided as template parameters."
  [{:keys [input]
    :as params}
   template]
  (with-open [reader (io/reader input)]
    (let [lines (csv/read-csv reader)
          head (first lines)
          header (map keyword head)
          invalid-variable-names (remove mustache/valid-variable-name? head)]
      (when (seq invalid-variable-names)
        (util/die (str "Invalid column names: "
                       (string/join ", " invalid-variable-names)
                       "\nOnly ASCII characters, ?, !, /, ., and - are allowed.")))
      (csv-seq params
               template
               (map (partial zipmap header) (next lines))
               lines))))
