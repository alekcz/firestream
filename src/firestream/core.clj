(ns firestream.core
  (:require [charmander.database :as charm]
            [clojure.core.async :as async]))


(defn producer 
  "Create a producer"
  [producer-path]
  (charm/init)
  (println (str "Created producer: " producer-path)))

(defn send! 
  "Send data to a producer"
  [producer topic data]
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
