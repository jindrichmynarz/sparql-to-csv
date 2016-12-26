(ns sparql-to-csv.spec
  (:require [clojure.spec :as s])
  (:import (java.io File Reader Writer)))

(s/def ::non-negative-int (s/and int? (complement neg?)))

(s/def ::positive-int (s/and int? pos?))

(s/def ::file (partial instance? File))

(s/def ::delimiter char?)

(s/def ::endpoint string?)

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
