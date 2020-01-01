(ns firestream.core
  (:require [charmander.database :as charm]
            [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [taoensso.timbre :as timbre]
            [clj-uuid :as uuid]))

(def root "firestream")

(def channel-len 8192)

(defn serialize-data [data]
  { :message (pr-str data)
    :timestamp (inst-ms (java.util.Date.))})

(defn deserialize-data [raw]
  (let [data (assoc (:data raw) :id (:id raw))]
    (assoc data :message (read-string (:message data)))))

(defn- clean-key [dirty-key]
  (-> (str dirty-key "")
      (str/replace  "." "!")
      (str/replace  "#" "!")
      (str/replace  "$" "!")
      (str/replace  "[" "!")
      (str/replace  "]" "!")))

(defn- pull-topic-data! 
  "Pull data from the topic"
  [consumer topic]
  (let [path (str (:path consumer) "/" (name topic)) not-consumed (str "consumed-by-" (:group.id consumer))]
      (charm/get-children path (:channel consumer) :order-by-child not-consumed :end-at 0)))

(defn producer 
  "Create a producer"
  [config]
  (charm/init)
  (let [server (clean-key (:bootstrap.servers config))]
    (timbre/info (str "Created producer connected to: " (str root "/" server)))
    {:path (str root "/" server)}))

(defn send! 
  "Send new message to topic"
  [producer topic data]
  (charm/set-object (str (:path producer) "/" (name topic) "/" (uuid/v1)) (serialize-data data)))

(defn consumer 
  "Create a consumer"
  [config]
  (charm/init)
  (let [consumer-path (str root "/" (clean-key (:bootstrap.servers config))) 
        group-id (clean-key (or (:group.id config) "default"))]
    (timbre/info  (str "Created consumer connected to: " consumer-path))
    (atom
      {:path consumer-path
        :group.id group-id
        :channel (async/chan channel-len)
        :topics []})))

(defn subscribe! 
  "Subscribe to a topic"
  [consumer topic]
      (swap! consumer #(assoc % :topics (conj (:topics %) topic)))
      (timbre/info  (str "Created consumer subscribed to: '" (name topic) "'"))
      (pull-topic-data! @consumer topic))
      
(defn poll! 
  "Read data from subscription"
  [consumer buffer-size]
  (let [available-data (map deserialize-data (filter some? (repeatedly buffer-size #(async/poll! (:channel @consumer)))))]
    (if (empty? available-data) 
      (doseq [topic (:topics @consumer)]
        (pull-topic-data! @consumer topic))
      available-data)))

(defn commit! 
  "Update offset for consumer in particular topic"
  [consumer topic firestream-object]
  (let [path (str (:path @consumer) "/" (name topic)) consumed-by (str "consumed-by-" (:group.id @consumer))]
  (charm/update-object (str path "/" (:id firestream-object)) {(keyword consumed-by) 1})))
  
(defn shutdown! [consumer]
  (let [data (async/into [] (:channel @consumer))]
    (async/close! (:channel @consumer))
    (reset! consumer nil)
    (timbre/info "Consumer shutdown")))