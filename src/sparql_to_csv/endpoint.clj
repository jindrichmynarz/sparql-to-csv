(ns sparql-to-csv.endpoint
  (:require [sparql-to-csv.util :as util]
            [sparql-to-csv.spec :as spec]
            [mount.core :as mount :refer [defstate]]
            [clj-http.client :as client]
            [clojure.string :as string]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn init-endpoint
  "Ping endpoint to test if it is up." 
  [{:keys [::spec/endpoint ::spec/max-retries ::spec/sleep]}]
  (try+ (let [virtuoso? (-> endpoint
                            (client/head {:throw-entire-message? true})
                            (get-in [:headers "Server"] "")
                            (string/includes? "Virtuoso"))]
          {:sparql-endpoint endpoint
           :max-retries max-retries
           :sleep sleep
           :virtuoso? virtuoso?})
        (catch [:status 404] _
          (throw+ {:type ::util/endpoint-not-found}))))

(defstate endpoint
  :start (init-endpoint (mount/args)))
