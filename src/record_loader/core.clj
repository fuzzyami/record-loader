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
            [clj-time.format :as f]
            [clj-gcloud.storage :as st]
            [clj-time.core :as time :refer [days ago now]])
  (:import java.util.zip.GZIPInputStream))

(defn gunzip
  "Writes the contents of input to output, decompressed.
  input: something which can be opened by io/input-stream.
      The bytes supplied by the resulting stream must be gzip compressed.
  output: something which can be copied to by io/copy."
  [input output & opts]
  (with-open [input (-> input io/input-stream GZIPInputStream.)]
    (apply io/copy input output opts)))


(defn from-ejson-to-clj-time [item]
    (coerce/from-string (:$date item)))

(def atom-counter (atom 0))
(def log-filename "./migration.log")
(def built-in-formatter (:mysql f/formatters))

(defn logline [record-type instance msg]
  (let [timestamp (f/unparse built-in-formatter (now))
        line (str timestamp "|" record-type "-" instance "|" msg "\n")]
    (spit log-filename line :append true)))

(defn read-records [filename]
  (-> (slurp filename)
      (clojure.string/split-lines)))

(defn get-metric-name [record-type]
  (cond
    (= record-type :cyco.edge-state) "edge"
    (= record-type :cyco.node-state) "node"))

(defn insert-record [config line record-type instance-num]
  (try
    (let [record (parse-string line)
          record-type (if (contains? record :src) :cyco.edge-state :cyco.node-state)
          assetname (if (= record-type :cyco.node-state) (:node-id record) (str (:src record) "->" (:dst record)))
          ;plug in the type metadata - either node or edge
          record (assoc record :-type- record-type)
          ;because mongo exports timestamps in its unique format, some conversion is needed:
          record (assoc record :to-time (from-ejson-to-clj-time (:to-time record)))
          record (assoc record :from-time (from-ejson-to-clj-time (:from-time record)))
          record (assoc record :created-at (from-ejson-to-clj-time (:created-at record)))
          record (assoc record :updated-at (from-ejson-to-clj-time (:updated-at record)))
          ;frdy serialized fields need explicit conversion:
          record (assoc record :samples (parse-string (:samples record)))
          record (assoc record :status (parse-string (:status record)))]
      (do
        (floob/add-record config record)
        (swap! atom-counter inc)
        (when (= 0 (mod @atom-counter 50000))
          (logline record-type instance-num (str "wrote asset: " assetname)))))
      (catch Exception e (do
                           (logline record-type instance-num (str "caught exception writing asset from line: " line ". " (.getMessage e)))
                           (logline record-type instance-num (with-out-str (clojure.stacktrace/print-stack-trace e)))))))

(defn download-file [filename record-type instance-num]
  (let [gz-filenae (str filename ".gz")]
    (try (io/delete-file filename) (catch Exception e))
    (try (io/delete-file gz-filenae) (catch Exception e))
    (logline record-type instance-num (str "downloading file " gz-filenae " from gcp..."))
    (st/download-file-from-storage (st/init {}) (str "gs://cyco-jesse-tmp/record-loader/" gz-filenae) gz-filenae)
    (logline record-type instance-num (str " ..unzipping.. " filename))
    (gunzip (io/file gz-filenae) (io/file filename))
    (logline record-type instance-num " ..done!")))

(def DD_AGENT_HOST "172.16.16.32")

(defn -main [record-type instance-num & args]
  (configure-logger! {:log-level :fatal})
  (let [filename (str record-type "-" instance-num)         ;edges-00 (or edges.00.gz)
        res (download-file filename record-type instance-num)
        config {:tinkles {:key "key" :url "http://engine-api.cycognito.com/tinkles/"}
                :hamurai {:project "cyco-main" :instance "qa-instance"}}]
    (floob/init-schemas config)
    (floob/init-hamurai (:hamurai config))
    (logline record-type instance-num "processing file...")
    (with-open [rdr (clojure.java.io/reader filename)]
      (doseq [line (line-seq rdr)] ;only read a line at a time as these are large files
        (insert-record config line record-type instance-num)))
    (logline record-type instance-num (str "done processing file:" filename ". total writes:" @atom-counter))))
