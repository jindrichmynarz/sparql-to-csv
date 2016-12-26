(ns sparql-to-csv.util
  (:require [clojure.string :as string]
            [clojure.java.io :as io]))

(derive ::decimal ::double)
(derive ::nonPositiveInteger ::integer)
(derive ::long ::integer)
(derive ::nonNegativeInteger ::integer)
(derive ::negativeInteger ::nonPositiveInteger)
(derive ::int ::long)
(derive ::short ::int)
(derive ::byte ::short)
(derive ::unsignedLong ::nonNegativeInteger)
(derive ::positiveInteger ::nonNegativeInteger)
(derive ::unsignedInt ::unsignedLong)
(derive ::unsignedShort ::unsignedInt)
(derive ::unsignedByte ::unsignedShort)

(defn ->double
  [s]
  (Double/parseDouble s))

(defn ->float
  [s]
  (Float/parseFloat s))

(defn ->integer
  [s]
  (Integer/parseInt s))

(defn- exit
  "Exit with `status` and message `msg`.
  `status` 0 is OK, `status` 1 indicates error."
  [^Integer status
   ^String msg]
  {:pre [(#{0 1} status)]}
  (println msg)
  (System/exit status))

(def die
  (partial exit 1))

(def info
  (partial exit 0))

(defn file-exists?
  "Test if file at `path` exists and is a file."
  [file]
  (and (.exists file) (.isFile file)))

(def join-lines
  (partial string/join \newline))

(defn lazy-cat'
  "Lazily concatenates a sequences `colls`.
  Taken from <http://stackoverflow.com/a/26595111/385505>."
  [colls]
  (lazy-seq
    (if (seq colls)
      (concat (first colls) (lazy-cat' (next colls))))))

(defn- prefix
  "Builds a function for compact IRIs in the namespace `iri`."
  [iri]
  (partial str iri))

(def xsd
  (prefix "http://www.w3.org/2001/XMLSchema#"))
