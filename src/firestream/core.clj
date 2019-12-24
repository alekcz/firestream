(ns firestream.core
  (:require [charmander.database :as charm]
            [cheshire.core :as json]
            [clojure.core.async :as async]))

(def root "firestream")

(defn make-channel 
  ([]
  (async/chan (async/dropping-buffer 8192)))
  ([n]
  (async/chan (async/dropping-buffer n))))

(defn- serialize-data [data]
  { :message (pr-str data)
    :processed false
    :timestamp (inst-ms (java.util.Date.))})

(defn- deserialize-data [raw]
  (let [data (assoc (:data raw) :id (:id raw))]
    (assoc data :message (read-string (:message data)))))

(defn producer 
  "Create a producer"
  [producer-path]
  (charm/init)
  (charm/delete-object "datoms")
  (charm/delete-object "datomic ")
  (println (str "Created producer: " producer-path))
  {:path (str root "/" producer-path)})

(defn send! 
  "Send data to a producer"
  [producer topic data]
  (charm/push-object (str (:path producer) "/" (name topic)) (serialize-data data))
  (println (str "Sent " data " to "  producer " under topic: " topic)))

(defn consumer 
  "Create a consumer"
  [consumer-path id channel]
  (charm/init)
  (println (str "Created consumer:" consumer-path))
  (atom {:path (str root "/" consumer-path)
   :groupid id
   :channel channel
   :topic nil}))

(defn subscribe! 
  "Send data to a producer"
  [consumer topic]
  (do 
    (swap! consumer #(assoc % :topic (name topic)))
    (charm/listen-to-child-added (str (:path @consumer) "/" (:topic @consumer)) (:channel @consumer) :order-by-key "processed" :equals false)))
      
(defn poll! 
  "Read data from subscription"
  [consumer buffer]
  (map deserialize-data (filter some? (repeatedly buffer #(async/poll! (:channel @consumer))))))