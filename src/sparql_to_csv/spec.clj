(ns sparql-to-csv.spec
  (:require [clojure.spec :as s])
  (:import (java.io File Reader Writer)
           (org.apache.commons.validator.routines UrlValidator)))

(def non-negative?
  (complement neg?))

(def http?
  (partial re-matches #"^https?:\/\/.*$"))

(def valid-url?
  "Test if `url` is valid."
  (let [validator (UrlValidator. UrlValidator/ALLOW_LOCAL_URLS)]
    (fn [url]
      (.isValid validator url))))

(s/def ::non-negative-int (s/and int? non-negative?))

(s/def ::positive-int (s/and int? pos?))

(s/def ::file (partial instance? File))

(s/def ::delimiter char?)

(s/def ::endpoint (s/and string? http? valid-url?))

(s/def ::extend? boolean?)

(s/def ::input (s/or :file ::file
                     :reader (partial instance? Reader)))

(s/def ::max-retries ::positive-int)

(s/def ::output (s/or :file ::file
                      :writer (partial instance? Writer)))

(s/def ::page-size ::positive-int)

(s/def ::parallel? boolean?)

(s/def ::sleep ::non-negative-int)

(s/def ::start-from ::non-negative-int)

(s/def ::config (s/keys :req [::delimiter ::endpoint ::extend? ::input ::max-retries
                              ::output ::page-size ::parallel? ::sleep ::start-from]))
