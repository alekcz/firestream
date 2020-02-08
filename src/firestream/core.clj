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

(defn serialize-data [topic key data]
  { :value (pr-str data)
    :key (or (deep-clean key) "k0")
    :topic topic
    :partion 0
    :offset 0
    :timestamp (inst-ms (java.util.Date.))})

(defn deserialize-data [raw]
  (let [data (-> raw :data) no-value (nil? (-> raw :data :value))]
    (if (or (nil? data) no-value)
      nil
      (assoc data 
          :value (read-string (:value data))
          :firestream-id (:id raw)))))

(defn distinct-and-ordered [coll]
  (let [unique (into #{} coll)]
    (sort-by :firestream-id (into () unique))))

(defn- get-available-data [consumer]
  (let [not-consumed (keyword (str "consumed-by-" (:group.id consumer)))]
    (distinct-and-ordered 
      (filter #(not (contains? % not-consumed))
        (filter some? 
          (map deserialize-data 
            (repeatedly 100 #(async/poll! (:channel consumer)))))))))

(defn pull-topic-data! 
  "Pull data from the topic"
  [consumer topic]
  (let [path (str (:path consumer) "/" (name topic)) not-consumed (str "consumed-by-" (:group.id consumer))]
      (charm/get-children path (:channel consumer) :order-by-child not-consumed :end-at 0)))

(defn- stream-topic-data! 
  "Stream data from the topic"
  [consumer topic]
  (let [path (str (:path consumer) "/" (name topic)) not-consumed (str "consumed-by-" (:group.id consumer))]
      (charm/listen-to-child-added path (:channel consumer) :order-by-child not-consumed :end-at 0)))

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
  (charm/set-object (str (:path producer) "/" (name topic) "/" (uuid/v1)) (serialize-data topic key value)))

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
        :topics (atom #{})
        :listeners (atom #{})}))

(defn subscribe! 
  "Subscribe to a topic"
  [consumer topic]
      (if (not (contains? (deref (:listeners consumer)) topic)) (stream-topic-data! consumer topic))        
      (swap! (:listeners consumer) #(conj % topic))
      (timbre/info  (str "Created consumer subscribed (firestream) to: '" (name topic) "'"))
      (swap! (:topics consumer) #(conj % topic))
      (get-available-data consumer)) 

(defn poll! 
  "Read data from subscription"
  [consumer timeout]
  (let [available-data (get-available-data consumer)]
    (if (empty? available-data) 
      (do
        (Thread/sleep timeout)
        (doseq [topic (deref (:topics consumer))]
          (pull-topic-data! consumer topic))
        (get-available-data consumer)) 
      available-data)))

(defn commit! 
  "Update offset for consumer in particular topic"
  [consumer topic firestream-object]
  (if (not (nil? firestream-object))
    (let [path (str (:path consumer) "/" (name topic)) consumed-by (str "consumed-by-" (:group.id consumer))]
      (charm/update-object (str path "/" (:firestream-id firestream-object)) {(keyword consumed-by) 1}))))
  
(defn shutdown! [consumer]
  (let [data (async/into [] (:channel consumer))]
    (async/close! (:channel consumer))
    (timbre/info "Consumer shutdown")))