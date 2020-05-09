(ns firestream.core
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fire.auth :as fire-auth]
            [fire.core :as fire]
            [incognito.edn :refer [read-string-safe]]
            [hasch.core :refer [uuid]]
            [clj-uuid :as uuid]
            [durable-queue :as dq]
            [taoensso.timbre :as timbre]))

(set! *warn-on-reflection* 1)

(def q (atom nil))
(def consumer-max (atom 16384))
(def root (atom "/firestream2"))
(def maxi (* 9.5 1024 1024))

(defn- serialize [data]
  (pr-str data))

(defn- deserialize [data']
  (let [value (read-string-safe {} (:data data'))]
    (-> data'
      (dissoc :data)
      (assoc :value value))))

(defn- deep-clean [stain]
  (str/replace (str stain) #"^(?![a-zA-Z\d-:_])" ""))

(defn- extract-data [cluster]
  (apply merge (map #(identity {(keyword (:id %)) (dissoc % :producer :topic :root)}) cluster)))

(defn- send-topic [cluster]
  (let [leader (first cluster)
        topic (:topic leader) p (:producer leader)
        root (:root leader) path (str root "/events/" topic)
        dataset (extract-data cluster)]
    (fire/update! (:db p) path dataset (:auth p) {:async true :print "silent"})
    (doseq [d dataset]
      (fire/write! (:db p) (str path "/" (-> d second :id) "/received-ms")  {".sv" "timestamp"} (:auth p) {:async true :print "silent"}))))

(defn- background-sender! []
  (let [slots (range 10000)
        t' (filter #(not= :timed-out %) (doall (map (fn [_] (dq/take! @q :firestream 0.05 :timed-out)) slots)))]
    (when (seq t')
      (let [t (doall (map #(deref %) t'))
            clusters (vals (group-by :topic t))]
        (doall (map send-topic clusters))
        (doall (map dq/complete! t'))
        nil))
    (count t')))

(defn- sender! []
  (timbre/info (str "Starting producer background thread"))
  (let [control (atom true)]
    (async/thread
      (loop []
        (try
          (background-sender!)
          (Thread/sleep 500)
          (catch Exception e (.printStackTrace e)))
        (when @control (recur))))
    (fn []
      (not (reset! control false)))))

(defn set-root [new-root]
  (reset! root (str @root "-" (deep-clean new-root))))

(defn producer 
  "Create a producer"
  [config]
  (let [auth (fire-auth/create-token (:env config))
        db (or (:bootstrap.servers config) (:project-id auth))]
    (when (str/blank? db) 
      (throw (Exception. ":bootstrap.servers cannot be empty. Could not detect :bootstrap.servers from service account")))
    (when (nil? @q) 
      (reset! q (dq/queues "/tmp/firestream" {})))
    (timbre/info (str "Created producer connected to: "  db ".firebaseio.com" @root))
    {:path @root
     :db db       
     :auth (dissoc auth :new-token)
     :shutdown (sender!)}))

(defn send! 
  "Send new message to topic"
  ([producer topic key value]
    (send! producer topic key value nil))
  ([producer topic key value unique]
    (let [time (uuid/get-timestamp (uuid/v1))
          id  (if (nil? unique) (str "e" time) (str (uuid {:key key :value value})))
          task {:data (serialize value)
                :id id
                :topic (name topic)
                :created-ms (System/currentTimeMillis)
                :root @root
                :key key
                :producer (dissoc producer :shutdown :res)}]
      (async/go (dq/put! @q :firestream task))
      nil)))

(defn consumer 
  "Create a consumer"
  [config]
  (let [auth (fire-auth/create-token (:env config))
        db (or (:bootstrap.servers config) (:project-id auth))
        group-id (str (deep-clean (or (:group.id config) "default")))
        read-handlers (get config :read-handlers (atom {}))]
    (when (str/blank? db) 
      (throw (Exception. ":bootstrap.servers cannot be empty. Could not detect :bootstrap.servers from service account")))
    (timbre/info (str "Created consumer connected to: "  db ".firebaseio.com" @root))
    {:path @root
      :group-id group-id
      :db db
      :offsets (atom {})
      :read-handlers read-handlers 
      :topics (atom #{})
      :auth auth}))


(defn- fetch-topic [consumer topic timeout]
  (let [timeout-ch (async/timeout timeout) 
        offset' (-> consumer :offsets deref topic)
        offset (or (:offset offset') offset')
        query (if (nil? offset) {:orderBy "$key"} {:orderBy "$key" :startAt (name offset)})
        res (async/alts!! 
              [timeout-ch 
              (fire/read 
                  (:db consumer) 
                  (str (:path consumer) "/events/" (name topic)) 
                  (:auth consumer) 
                  {:query query :async true})])
        data' (-> res first vals vec)
        data  (sort-by :id (for [d data'] (deserialize (into {} d))))
        channel (second res)]
    (if (= channel timeout-ch)
      []
      (if (nil? offset) data (rest data)))))

(defn poll! 
  "Read data from subscription"
  [consumer timeout]
  (apply merge {} (map #(identity {% (fetch-topic consumer % timeout)}) (deref (:topics consumer)))))

(defn subscribe! 
  "Subscribe to a topic"
  [consumer topic]
  (let [topic (name topic)
        ktopic (keyword topic)
        offset (fire/read (:db consumer) 
                  (str (:path consumer) "/consumers/" (:group-id consumer) "/offsets/" topic) (:auth consumer))]
  (swap! (:topics consumer) #(conj % ktopic))
  (swap! (:offsets consumer) #(assoc % ktopic offset))
  (fetch-topic consumer ktopic 1000)))

(defn unsubscribe! 
  "Unsubscribe to a topic"
  [consumer topic]
    (swap! (:topics consumer) #(disj % topic)))

(defn commit! 
  "Update offset for consumer in particular topic"
  [consumer offset-map]
  (let [topic (name (:topic offset-map))
        ktopic (keyword topic)
        offset (:offset offset-map)
        metadata (merge (:metadata offset-map) {})]
  (fire/update! 
    (:db consumer) 
    (str (:path consumer) "/consumers/" (:group-id consumer) "/offsets/" topic)
    {:offset offset :metadata (merge metadata  {:committed-ms (System/currentTimeMillis)})} 
    (:auth consumer))
  (swap! (:offsets consumer) #(assoc % ktopic offset))))
  
(defn shutdown!
  "Shutdown producer background thread"
  [producer]
  (timbre/info (str "Shutting down producer"))
  ((:shutdown producer)))
  
