(defproject sparql-to-csv "0.1.0-SNAPSHOT"
  :description "Stream SPARQL results to CSV"
  :url "http://github.com/jindrichmynarz/sparql-to-csv"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/data.zip "0.1.2"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.clojure/data.csv "0.1.3"]
                 [stencil "0.5.0"]
                 [clj-http "3.4.1"]
                 [commons-validator/commons-validator "1.5.1"]
                 [slingshot "0.12.2"]
                 [mount "0.1.11"]
                 ;[im.chit/hara.io.file "2.4.8"]
                 ;[com.taoensso/timbre "4.8.0"]
                 [org.apache.jena/jena-core "3.1.1"]
                 [org.apache.jena/jena-arq "3.1.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.1"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]]
  :main sparql-to-csv.cli
  :profiles {:uberjar {:aot :all
             :uberjar-name "sparql_to_csv.jar"}})
