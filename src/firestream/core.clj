(ns firestream.core
  (:require [charmander.database :as charm]
            [clojure.core.async :as async]))

(def root "streams")

(defn- serialize-data [data]
  { :data (pr-str data)
    :timestamp (inst-ms (java.util.Date.))})

(defn- deserialize-data [raw]
  (read-string (:data raw)))

(defn producer 
  "Create a producer"
  [producer-path]
  (charm/init)
  (charm/delete-object "datoms")
  (charm/delete-object "datomic")
  (println (str "Created producer: " producer-path))
  {:path (str root "/" producer-path)})

(defn send! 
  "Send data to a producer"
  [producer topic data]
  (charm/push-object (str (:path producer) "/" (name topic)) (serialize-data data))
  (println (str "Sent " data " to "  producer " under topic: " topic)))

(defn consumer 
  "Create a consumer"
  [consumer-path]
  (charm/init)
  (println (str "Created consumer:" consumer-path)))

(defn subscribe! 
  "Send data to a producer"
  [consumer topic]
  (println (str "Consuming data from "  consumer " under topic: " topic)))

(defn poll! 
  "Read data from subscription"
  [consumer buffer]
  (println (str "Consuming up to " buffer " from " consumer)))
