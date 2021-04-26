(ns record-loader.core
  (:gen-class)
  (:require [floob.core :as floob]
            [hamurai.component :as hamurai]
            [hamurai.core :as hamcore]
            [record-loader.jvm :as jvm]
            [com.stuartsierra.component :as component]
            [environ.core :refer [env]]
            [cbass :refer [delete-by find-by lazy-scan pack-un-pack scan store store-batch]]
            [kruger.frdy2.core :refer [parse-string]]
            [pencilvester.core :refer [configure-logger!]]
            [cbass :refer [new-connection store find-by scan delete]]
            [clj-time.core :as time :refer [days ago]])
  (:import com.google.cloud.bigtable.hbase.BigtableConfiguration
           datadog.trace.api.DDTags
           org.apache.hadoop.hbase.client.Connection
           (java.util Random)))

(defn from-ejson-to-clj-time [item]
  (clj-time.coerce/from-string(:$date item)))

(defn read-records [filename]
				(-> (slurp filename)
    (clojure.string/split-lines)))

(defn write-to-file [filename string]
  (spit filename string :append true))

(defn get-free-mem []
  (float (/ (-> (java.lang.Runtime/getRuntime)
                (.freeMemory)) 1024)))

(def a-very-long-text "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Quisque euismod tincidunt massa eu sollicitudin. Sed sollicitudin odio eu auctor posuere. Sed sit amet velit turpis. Vivamus blandit sit amet risus sed mollis. Quisque eu justo id felis malesuada blandit. Mauris faucibus ipsum ac dolor vulputate, id molestie ante elementum. Sed aliquet lacinia diam, a tincidunt libero suscipit at. Cras non magna libero. Proin quis risus ex. Vivamus id consequat ex. Vivamus id condimentum nisi. Nullam molestie, sem et blandit accumsan, tellus arcu lacinia lectus, sit amet iaculis leo velit varius nisi. Pellentesque tortor eros, pellentesque in egestas vitae, molestie a magna.\n\nDuis tempus, metus eget euismod pulvinar, nisl nibh imperdiet felis, nec molestie tellus tortor et sem. Vivamus sit amet sodales lectus, et molestie erat. Suspendisse potenti. Nunc id dui volutpat, posuere quam id, laoreet ipsum. Curabitur mollis consequat tortor sed malesuada. Vivamus sed iaculis dolor, vel convallis leo. In a sem vel sapien pharetra semper at imperdiet ipsum. Etiam bibendum elit id blandit congue. Vivamus hendrerit nunc leo, non aliquet est facilisis ac. Nullam condimentum nisl lacus, et ultrices magna accumsan quis. Aliquam non dolor et dolor ultrices efficitur. Proin massa nisl, ullamcorper vitae molestie non, pellentesque sed sem. Proin ex nisi, rhoncus blandit est quis, bibendum dignissim purus. Nulla aliquam, ipsum eget auctor hendrerit, nisl sem varius felis, sed fermentum sapien ligula ac velit. Nam feugiat placerat dictum. Vivamus porta ut eros sit amet pellentesque.\n\nMauris nec risus magna. Sed dui mauris, iaculis suscipit lacus non, laoreet iaculis felis. Praesent ullamcorper, dolor in sollicitudin posuere, libero diam cursus ante, nec vestibulum tellus nulla quis orci. Praesent dolor orci, aliquet a facilisis sit amet, pretium at orci. Nunc id dolor ipsum. Praesent auctor faucibus quam, quis cursus nulla vehicula ac. Quisque ornare augue metus, imperdiet luctus magna faucibus nec. Donec vitae turpis commodo, viverra elit vitae, sagittis ipsum. Sed non ipsum felis. Donec accumsan ex lacus, in semper tellus rutrum non. Nunc ultricies sapien ut lacinia feugiat. Sed eros quam, finibus et tristique sed, venenatis sit amet sapien. Aenean tempus, nibh nec blandit tristique, dui ipsum gravida turpis, elementum gravida lectus lectus et augue.")

(defn data-to-insert [index]
  {:node-id (str a-very-long-text a-very-long-text a-very-long-text a-very-long-text a-very-long-text)}) ;(str "node-id-" index)})

(def node-schema
  {:families {"data" {:default {:value-type :string}
                                            :columns {;"samples" {:s11n :frdy}
                                                      "node-id" {:value-type :string}
                                                      ;"from-time" {:value-type :time}
                                                      ;"to-time" {:value-type :time}
                                                      ;"updated-at" {:value-type :time}
                                                      ;"created-at" {:value-type :time}
                                                      ;"status" {:s11n :frdy}
                                                      }}}
   :row-key  {:key-type          :compound
              :delimiter         "@"
              :parts             [{:key-type :string
                                   :length   :variable}]
              :primary-key-parts #{:node_id}}})

(defn command [index]
  {:tablename "krazy-nodes"
   :schema node-schema
   :row-key (str "node-" index "-" a-very-long-text)
   :columns (data-to-insert index) })

(defn call-hamurai [hamurai-instance index]
  (write-to-file "mem.log" (str (get-free-mem) "\n"))
  (hamcore/set-row! hamurai-instance "krazy-nodes" (command index)))


(defn make-row [rowkey-prefix index]
  (let [rowkey (str rowkey-prefix index)]
  [rowkey "data" {rowkey a-very-long-text}]))

(defn insert-record [config line]
  (write-to-file "mem.log" (str (get-used-mem) "\n"))

  (let [record (parse-string line)
        record (if (contains? record :src) ;plug in the type - either node or edge
                 (assoc record :-type- :cyco.edge-state)
                 (assoc record :-type- :cyco.node-state))
        record (assoc record :to-time (from-ejson-to-clj-time (:to-time record)))
        record (assoc record :from-time (from-ejson-to-clj-time (:from-time record)))
        record (assoc record :created-at (from-ejson-to-clj-time (:created-at record)))
        record (assoc record :updated-at (from-ejson-to-clj-time (:updated-at record)))
        record (assoc record :samples (parse-string (:samples record)))
        record (assoc record :status (parse-string (:status record)))]
    (try
      (floob/add-record config record)
    (catch Exception e (str "caught exception: " (.getMessage e))))))

(def old-file "./collection.json")
(def new-file "/Users/amiblonder-minerva/Work/record-loader/record-loader/dump.json")

(defn -main [& args]
  (configure-logger! {:log-level :fatal})
  (let [config {:tinkles {:key "key" :url "http://engine-api.cycognito.com/tinkles/"}
                :hamurai {:project "cyco-main" :instance "qa-instance"}}
        ;lines (doall (read-records new-file))
        ham (component/start (hamurai/map->Hamurai (:hamurai config)))]
    ;(floob/init-schemas config)
    ;(floob/init-hamurai (:hamurai config))
    ;(doseq [index (range 0 (count lines))]
    ;  (insert-record config (nth lines index)))
    (do
      (System/gc)
      (jvm/heap-dump "./heap-dump-before.bin" false)
      (doseq [index (range 0 (Integer/parseInt (first args)))]
        (do
          (call-hamurai ham index))
          (write-to-file "mem.log" (str (get-free-mem) "\n")))
      (do
        (System/gc)
        (jvm/heap-dump "./heap-dump-after.bin" false))
      )))
