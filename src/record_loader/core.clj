(ns record-loader.core
  (:gen-class)
  (:require [floob.core :as floob]
            [environ.core :refer [env]]
            [clj-time.coerce :as coerce]
            [cbass :refer [delete-by find-by lazy-scan pack-un-pack scan store store-batch]]
            [kruger.frdy2.core :refer [parse-string]]
            [pencilvester.core :refer [configure-logger!]]
            [clojure.java.io :refer [file output-stream input-stream] :as io]
            [cbass :refer [new-connection store find-by scan delete]]
            [cognician.dogstatsd :as dogstatsd]
            [clj-gcloud.storage :as st]
            [clj-time.core :as time :refer [days ago]])
  (:import com.google.cloud.bigtable.hbase.BigtableConfiguration
           datadog.trace.api.DDTags
           java.util.zip.GZIPInputStream
           java.util.zip.GZIPOutputStream
           org.apache.hadoop.hbase.client.Connection
           (java.util Random)))

(defn gunzip
  "Writes the contents of input to output, decompressed.
  input: something which can be opened by io/input-stream.
      The bytes supplied by the resulting stream must be gzip compressed.
  output: something which can be copied to by io/copy."
  [input output & opts]
  (with-open [input (-> input io/input-stream GZIPInputStream.)]
    (apply io/copy input output opts)))


(defn from-ejson-to-clj-time [item]
  (try
    (coerce/from-string (:$date item))
    (catch Exception e (do
                         (str "caught exception: " (.getMessage e))
                         (dogstatsd/increment! "ts-conversion" 1)))))


(defn read-records [filename]
				(-> (slurp filename)
    (clojure.string/split-lines)))

(defn get-metric-name [record-type]
  (cond
    (= record-type :cyco.edge-state) "edge"
    (= record-type :cyco.node-state) "node"))

(defn insert-record [config line index]
  (let [record (parse-string line)
        record-type (if (contains? record :src) :cyco.edge-state :cyco.node-state)

        ;plug in the type metadata - either node or edge
        record  (assoc record :-type- record-type)
        ;because mongo exports timestamps in its unique format, some conversion is needed:
        record (assoc record :to-time (from-ejson-to-clj-time (:to-time record)))
        record (assoc record :from-time (from-ejson-to-clj-time (:from-time record)))
        record (assoc record :created-at (from-ejson-to-clj-time (:created-at record)))
        record (assoc record :updated-at (from-ejson-to-clj-time (:updated-at record)))
        ;frdy serialized fields need explicit conversion:
        record (assoc record :samples (parse-string (:samples record)))
        record (assoc record :status (parse-string (:status record)))]
    (try
      (do
        (floob/add-record config record)
        (when (= 0 (mod index 100))
          (do
            (println (str "asset: " (if (= record-type :cyco.node-state) (:node-id record) (str (:src record) "-" (:dst record)))))
            (println (str "wrote " index " items"))))
            (dogstatsd/increment! (str "loaded." (get-metric-name record-type)) 1))
    (catch Exception e (do
                         (dogstatsd/increment! (str "failed1." (get-metric-name record-type)) 1)
                         (str "caught exception: " (.getMessage e)))))))

(defn download-file [filename]
  (let [gz-filenae (str filename ".gz")]
    (try (io/delete-file filename) (catch Exception e))
    (try (io/delete-file gz-filenae) (catch Exception e))
    (print (str "downloading file " gz-filenae " from gcp..."))
    (st/download-file-from-storage (st/init {}) (str "gs://cyco-jesse-tmp/record-loader/" gz-filenae) gz-filenae)
    (print (str " ..unzipping.. " filename))
    (gunzip (io/file gz-filenae) (io/file filename))
    (println " ..done!")))

(def DD_AGENT_HOST "172.16.16.79")

(defn -main [attempt-num type instance-num & args]
  ;download file from bucket
  (dogstatsd/configure! (str DD_AGENT_HOST ":8125") {:tags {:service "record.loader" :attempt attempt-num :type type :instance instance-num}})
  (configure-logger! {:log-level :fatal})
  (let [filename (str type "-" instance-num ".json")
        res (download-file filename)
        config {:tinkles {:key "key" :url "http://engine-api.cycognito.com/tinkles/"}
                :hamurai {:project "cyco-main" :instance "qa-instance"}}
        lines (doall (read-records filename))]
    (floob/init-schemas config)
    (floob/init-hamurai (:hamurai config))
    (doseq [index (range 0 (count lines))]
        (insert-record config (nth lines index) index))))
