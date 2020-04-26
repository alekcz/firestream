(ns firestream.core
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fire.auth :as fire-auth]
            [fire.core :as fire]
            [incognito.edn :refer [read-string-safe]]
            [hasch.core :refer [uuid]]
            [com.climate.claypoole :as cp]
            [durable-queue :as dq]))

(set! *warn-on-reflection* 1)

(def q (dq/queues "/tmp/firestream" {}))

(def maxi (* 9.5 1024 1024))

(defn serialize [id data]
  (let [data' (pr-str data)
        size (count data')]
    (if (< size maxi)
      {:data data' :id id}
      (throw (Exception. (str "Maximum value size exceeded!: " size))))))

(defn deserialize [data' read-handlers]
   {:data (read-string-safe read-handlers (:data data'))
    :id (:id data')})

(def root (atom "/firestream"))

(def channel-len 8192)

(defn- deep-clean [stain]
  (str/replace (str stain) #"^(?![a-zA-Z\d-_])" ""))

(defn set-root [new-root]
  (reset! root (str "firestream-" (deep-clean new-root))))

(defn producer 
  "Create a producer"
  [config]
  (let [db (:bootstrap.servers config)
        server (uuid db)
        auth (fire-auth/create-token (:env config))]
    (println (str "Created producer connected to: " (str @root "/" server)))
    {:path (str @root "/" server)
     :db db       
     :auth auth}))

(defn consumer 
  "Create a consumer"
  [config]
  (let [db (:bootstrap.servers config)
        read-handlers (get config :read-handlers (atom {}))
        consumer-path (str @root "/" (uuid db)) 
        group-id (uuid (or (:group.id config) "default"))
        auth (fire-auth/create-token (:env config))]
    (println  (str "Created consumer connected to: " consumer-path))
      {:path consumer-path
       :group.id group-id
       :db db
       :read-handlers read-handlers 
       :auth auth}))

(defn send! 
  "Send new message to topic"
  ([producer topic key value]
    (send! producer topic key value nil))
  ([producer topic key value unique]
    (let [noise (if (nil? unique) {:time (inst-ms (java.util.Date.)) :r (rand-int 1000)} nil)
          id (uuid {:key key :noise nil})
          task {:data (serialize id value)
                :id id
                :topic topic
                :root @root
                :producer producer}]
      (dq/put! q :firestream task))))

(defn background-sender! []
  (let [t (dq/take! q :firestream)
        p (:producer t) data (:data t) id (:id t) root (:root t) topic (:topic t)]
    (fire/write! (:db p) (str @root "/topics/" topic "/" id) data (:auth p))))

(defn sen [p data] 
  (send! p :quotes :k0 data))

(defn gg [n]
  (apply merge {} (map (fn [k] {(keyword (str "t" k)) k}) (range n))))

(defn bench [p data]
  (time 
    (do
      (fire/write! (:db p) (str @root "/topics") data (:auth p))
      nil)))
  ;(time (do (doall (cp/pmap 500 #(sen c %) (range n))) nil)))

(defn pull-topic-data! 
  "Pull data from the topic"
  [consumer topic]
  nil)

(defn subscribe! 
  "Subscribe to a topic"
  [consumer topic]
  (swap! (:topics consumer) #(conj % topic))
  (pull-topic-data! consumer topic)) 

(defn unsubscribe! 
  "Unsubscribe to a topic"
  [consumer topic]
    (swap! (:topics consumer) #(disj % topic)))

(defn poll! 
  "Read data from subscription"
  [consumer timeout]
  nil)

(defn commit! 
  "Update offset for consumer in particular topic"
  [consumer topic firestream-object]
  nil)
  
(defn shutdown! [consumer]
 nil)