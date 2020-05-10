(ns firestream.core
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fire.auth :as fire-auth]
            [fire.core :as fire]
            [incognito.edn :refer [read-string-safe]]
            [hasch.core :refer [uuid]]
            [clj-uuid :as uuid]))

(set! *warn-on-reflection* 1)

(def root (atom "/firestream2"))

(defn- serialize [data]
  (pr-str data))

(defn- deserialize [data']
  (let [value (read-string-safe {} (:data data'))]
    (-> data'
      (dissoc :data)
      (assoc :value value))))

(defn- deep-clean [stain]
  (str/replace (str stain) #"^(?![a-zA-Z\d-:_])" ""))

(defn set-root [new-root]
  (reset! root (str @root "-" (deep-clean new-root))))

(defn producer 
  "Create a producer"
  [config]
  (let [auth (fire-auth/create-token (:env config))
        db (or (:bootstrap.servers config) (:project-id auth))]
    (when (str/blank? db) 
      (throw (Exception. ":bootstrap.servers cannot be empty. Could not detect :bootstrap.servers from service account")))
    (println (str "Created producer connected to: "  db ".firebaseio.com" @root))
    {:path @root
     :db db       
     :auth (dissoc auth :new-token)}))

(defn send! 
  "Send new message to topic"
  ([producer topic key value]
    (send! producer topic key value nil))
  ([producer topic key value unique]
    (let [time (uuid/get-timestamp (uuid/v1))
          id  (if (nil? unique) (str "e" time) (str (uuid {:key key :value value})))
          path (str @root "/events/" (name topic))
          db (:db producer)
          auth (:auth producer)
          task {:data (serialize value)
                :id id
                :created-ms (System/currentTimeMillis)
                :key key}]
      [(fire/update! db path {id task} auth {:async true :print "silent"})
       (fire/write! db (str path "/" id "/received-ms")  {".sv" "timestamp"} auth {:async true :print "silent"})])))

(defn consumer 
  "Create a consumer"
  [config]
  (let [auth (fire-auth/create-token (:env config))
        db (or (:bootstrap.servers config) (:project-id auth))
        group-id (str (deep-clean (or (:group.id config) "default")))
        read-handlers (get config :read-handlers (atom {}))]
    (when (str/blank? db) 
      (throw (Exception. ":bootstrap.servers cannot be empty. Could not detect :bootstrap.servers from service account")))
    (println (str "Created consumer connected to: "  db ".firebaseio.com" @root))
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
