(ns sparql-to-csv.util
  (:require [clojure.string :as string]))

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
