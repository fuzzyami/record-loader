(defproject record-loader "0.1.0"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [com.cycognito/hamurai "0.0.32"]
                 [com.cycognito/floob-clj "0.2.9-write-only-to-bt"]
                 [com.stuartsierra/component "1.0.0"]
                 [ring "1.8.2"]
                 [clj-http "3.12.0"]
                 [environ "1.2.0"]
                 [cheshire "5.10.0"]
                 [cognician/dogstatsd-clj]
                 [com.clojure-goes-fast/clj-memory-meter "0.1.3"]
                 [com.cycognito/pencilvester "0.0.21"]
                 [com.cycognito/kruger "2.0.14"]
                 [com.taoensso/tufte "2.2.0"]
                 [viebel/cbass "0.2.11" :exclusions [[org.apache.hbase/hbase-shaded-client]]]
                 [com.google.cloud.bigtable/bigtable-hbase-1.x "1.20.0" :exclusions [[org.slf4j/slf4j-log4j12]] ]
                 [com.taoensso/encore "3.10.1"]
                 [com.oscaro/clj-gcloud-storage "0.71-1.2"]]
  ;:main ^:skip-aot record-loader.core
  :main record-loader.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :plugins [[lein-parent "0.3.8"]]
  :repositories [
                 ["snapshots" {:url      "https://maven.cyco.fun/snapshots"
                             :username :env/cyco_maven_username
                             :password :env/cyco_maven_password}]
               ["releases" {:url      "https://maven.cyco.fun/releases"
                            :username :env/cyco_maven_username
                            :password :env/cyco_maven_password}]]

  :parent-project {:coords [com.cycognito/cyco-parent-clj "0.0.154"]
                   :inherit [:dependencies
                             :managed-dependencies
                             :plugins
                             :bom]}
  ;:jvm-opts ["-Djdk.attach.allowAttachSelf"]
  :ring {:handler record-loader.core/handler})