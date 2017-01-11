(ns sparql-to-csv.endpoint
  (:require [sparql-to-csv.util :as util]
            [sparql-to-csv.spec :as spec]
            [mount.core :as mount :refer [defstate]]
            [clj-http.client :as client]
            [clojure.string :as string]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn init-endpoint
  "Ping endpoint to test if it is up." 
  [{::spec/keys [auth endpoint max-retries sleep]}]
  (try+ (let [virtuoso? (-> endpoint
                            (client/head (cond-> {:throw-entire-message? true}
                                           auth (assoc :digest-auth auth)))
                            (get-in [:headers "Server"] "")
                            (string/includes? "Virtuoso"))]
          {:sparql-endpoint endpoint
           :auth auth
           :max-retries max-retries
           :sleep sleep
           :virtuoso? virtuoso?})
        (catch [:status 401] _
          (throw+ {:type ::util/invalid-auth}))
        (catch [:status 404] _
          (throw+ {:type ::util/endpoint-not-found}))))

(defstate endpoint
  :start (init-endpoint (mount/args)))
