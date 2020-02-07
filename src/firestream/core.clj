(ns firestream.core
  (:require [charmander.database :as charm]
            [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [taoensso.timbre :as timbre]
            [clj-uuid :as uuid]))

(def root (atom "firestream"))

(def channel-len 8192)

(defn- clean-key [dirty-key]
  (-> (keyword dirty-key)
      (name)
      (str)(str/replace  "." "!")
      (str/replace  "#" "!")
      (str/replace  "$" "!")
      (str/replace  "[" "!")
      (str/replace  "]" "!")))

(defn- deep-clean [stain]
  (str/replace (str stain) #"^(?![a-zA-Z\d-_])" ""))

(defn set-root [new-root]
  (reset! root (str "firestream-" (deep-clean new-root))))

(defn serialize-data [key data]
  { :message (pr-str data)
    :key (pr-str (or (deep-clean key) :default))
    :timestamp (inst-ms (java.util.Date.))})

(defn deserialize-data [raw]
  (let [data (-> raw :data) no-message (nil? (-> raw :data :message))]
    (if (or (nil? data) no-message)
      nil
      (assoc data 
          :message (read-string (:message data))
          :key (read-string (:key data))
          :firestream-id (:id raw)))))

(defn prepare-and-deserialize-data [raw]
  (let [data (deserialize-data raw)]
    (if (nil? data)
      nil
      (assoc (:message data) :firestream-id (:firestream-id data)))))

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
    (timbre/info (str "Created producer connected to: " (str @root "/" server)))
    {:path (str @root "/" server)}))

(defn send! 
  "Send new message to topic"
  [producer topic key value]
  (charm/set-object (str (:path producer) "/" (name topic) "/" (uuid/v1)) (serialize-data key value)))

(defn consumer 
  "Create a consumer"
  [config]
  (charm/init)
  (let [consumer-path (str @root "/" (clean-key (:bootstrap.servers config))) 
        group-id (clean-key (or (:group.id config) "default"))]
    (timbre/info  (str "Created consumer connected to: " consumer-path))
      {:path consumer-path
        :group.id group-id
        :channel (async/chan channel-len)
        :topics (atom [])}))

(defn subscribe! 
  "Subscribe to a topic"
  [consumer topic]
      (swap! (:topics consumer) #(conj % topic))
      (timbre/info  (str "Created consumer subscribed to: '" (name topic) "'"))
      (pull-topic-data! consumer topic))
      
(defn poll! 
  "Read data from subscription"
  [consumer buffer-size]
  (let [available-data (filter some? (map prepare-and-deserialize-data  (repeatedly buffer-size #(async/poll! (:channel consumer)))))]
    (if (empty? available-data) 
      (doseq [topic (deref (:topics consumer))]
        (pull-topic-data! consumer topic))
      available-data)))

(defn commit! 
  "Update offset for consumer in particular topic"
  [consumer topic firestream-object]
  (let [path (str (:path consumer) "/" (name topic)) consumed-by (str "consumed-by-" (:group.id consumer))]
  (charm/update-object (str path "/" (:firestream-id firestream-object)) {(keyword consumed-by) 1})))
  
(defn shutdown! [consumer]
  (let [data (async/into [] (:channel consumer))]
    (async/close! (:channel consumer))
    (timbre/info "Consumer shutdown")))