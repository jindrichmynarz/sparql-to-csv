(ns sparql-to-csv.spec
  (:require [clojure.spec :as s]
            [clojure.string :as string])
  (:import (java.io File Reader Writer)
           (org.apache.commons.validator.routines UrlValidator)))

(def http?
  (partial re-matches #"^https?:\/\/.*$"))

(def non-negative?
  (complement neg?))

(def non-empty-string?
  (every-pred (complement empty?) string?))

(def valid-url?
  "Test if `url` is valid."
  (let [validator (UrlValidator. UrlValidator/ALLOW_LOCAL_URLS)]
    (fn [url]
      (.isValid validator url))))

(s/def ::non-negative-int (s/and int? non-negative?))

(s/def ::positive-int (s/and int? pos?))

(s/def ::file (partial instance? File))

(s/def ::auth (s/cat :username non-empty-string? :password non-empty-string?))

(s/def ::endpoint (s/and string? http? valid-url?))

(s/def ::extend? true?)

(s/def ::help? true?)

(s/def ::input (s/or :file ::file
                     :reader (partial instance? Reader)))

(s/def ::input-delimiter char?)

(s/def ::max-retries ::positive-int)

(s/def ::output (s/or :file ::file
                      :writer (partial instance? Writer)))

(s/def ::output-delimiter char?)

(s/def ::page-size ::positive-int)

(s/def ::parallel? true?)

(s/def ::skip-sparql-validation? true?)

(s/def ::sleep ::non-negative-int)

(s/def ::start-from ::non-negative-int)

(s/def ::config (s/keys :req [::endpoint ::input ::input-delimiter ::max-retries
                              ::output ::output-delimiter ::page-size ::sleep ::start-from]
                        :opt [::auth ::extend? ::help? ::parallel? ::skip-sparql-validation?]))
