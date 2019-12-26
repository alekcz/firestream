(ns firestream.core
  (:require [charmander.database :as charm]
            [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.string :as str]))

(def root "firestream")

(defn make-channel []
  (async/chan 8192))

(defn- serialize-data [data]
  { :message (pr-str data)
    :processed false
    :timestamp (inst-ms (java.util.Date.))})

(defn- deserialize-data [raw]
  (let [data (assoc (:data raw) :id (:id raw))]
    (assoc data :message (read-string (:message data)))))

(defn- clean-key [dirty-key]
  (-> (str dirty-key "")
      (str/replace  "." "!")
      (str/replace  "#" "!")
      (str/replace  "$" "!")
      (str/replace  "[" "!")
      (str/replace  "]" "!")))

(defn producer 
  "Create a producer"
  [config]
  (charm/init)
  (let [server (clean-key (:bootstrap.servers config))]
    (println (str "Created producer connected to: " (str root "/" server)))
    {:path (str root "/" server)}))

(defn send! 
  "Send new message to topic"
  [producer topic data]
  (charm/push-object (str (:path producer) "/" (name topic)) (serialize-data data))
  (println (str "Sent " data " to "  (:path producer) " under topic: " topic)))

(defn consumer 
  "Create a consumer"
  [config]
  (charm/init)
  (let [consumer-path (str root "/" (clean-key (:bootstrap.servers config))) 
        group-id (clean-key (or (:group.id config) "default"))
        channel (make-channel)]
    (println (str "Created consumer connected to:" consumer-path))
   ; (atom
    {:path consumer-path
      :group.id group-id
      :channel channel}))
      ;:topics []})))

(defn subscribe! 
  "Subscribe to a topic"
  [consumer topic]
    (let [path (str (:path consumer) "/" (name topic))]
      ;(swap! consumer #(assoc % :topics (conj (:topics %) topic)))
      (charm/listen-to-child-added path (:channel consumer) :order-by-key (str "processed") :equals false)))
      
(defn poll! 
  "Read data from subscription"
  [consumer buffer-size]
  (map deserialize-data (filter some? (repeatedly buffer-size #(async/poll! (:channel consumer))))))