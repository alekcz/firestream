(ns firestream.core
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fire.auth :as fire-auth]
            [fire.core :as fire]
            [incognito.edn :refer [read-string-safe]]
            [hasch.core :refer [uuid]]
            [clj-uuid :as uuid]))

(set! *warn-on-reflection* 1)
(def send-queue (async/chan (async/dropping-buffer 16384)))
(def expiry (atom (* 10 60 1000))) ;default to 10 min
(def firestream-root (atom "/firestream2"))

(defn- serialize [data]
  (pr-str data))

(defn- deserialize [data']
  (let [value (read-string-safe {} (:data data'))]
    (-> data'
      (dissoc :data)
      (assoc :value value))))

(defn- deep-clean [stain]
  (str/replace (str stain) #"^(?![a-zA-Z\d-:_/])" ""))

(defn- extract-data [cluster]
  (let [sent-time (System/currentTimeMillis)
        prep (fn [m] (-> m  (dissoc :producer :topic :root) (assoc :sent-ms sent-time)))]
    (apply merge (map #(identity {(keyword (:id %)) (prep %)}) cluster))))

(defn- send-topic [cluster]
  (let [leader (first cluster)
        topic (:topic leader) 
        p (:producer leader)
        root (:root leader) 
        path (str root "/events/" topic)
        dataset (extract-data cluster)
        auth (:auth p)
        now (inst-ms (java.util.Date.))]
     (when (> now (deref (:expiry p))) 
      (println "refreshing")
      (reset! (:auth p) (fire-auth/create-token (:env p)))
      (reset! (:expiry p) (+ @expiry (inst-ms (java.util.Date.)))))
    (fire/update! (:db p) path dataset @auth {:async false :print "silent"})))

(defn- background-sender! []
  (let [t (filter some? (repeatedly 8000 #(async/poll! send-queue)))]
    (when (seq t)
      (let [clusters (vals (group-by :topic t))]
        (doall (map send-topic clusters))))
    (count t)))

(defn- sender! []
  (let [control (atom true)]
    (async/thread
      (loop []
        (try
          (Thread/sleep 200)
          (background-sender!)
          (catch Exception e (.printStackTrace e)))
        (when @control (recur))))
    (fn []
      (not (reset! control false)))))

(defn set-root [new-root]
  (reset! firestream-root (str @firestream-root "-" (deep-clean new-root))))

(defn set-expiry [milli]
  (reset! expiry milli))

(defn producer 
  "Create a producer"
  [config]
  (let [auth (fire-auth/create-token (:env config))
        db (or (:bootstrap.servers config) (:project-id auth))
        root (str @firestream-root (or (:root config) "/default" ))]
    (when (str/blank? db) 
      (throw (Exception. ":bootstrap.servers cannot be empty. Could not detect :bootstrap.servers from service account")))
    (println (str "Created PRODUCER connected to: "  db ".firebaseio.com" root))
    (let [shutdown (sender!)]
      (.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable shutdown))
      {:root (deep-clean root)
      :db db   
      :env (:env config)     
      :expiry (atom (+ @expiry (inst-ms (java.util.Date.))))
      :auth (atom auth)
      :shutdown shutdown})))

(defn send! 
  "Send new message to topic"
  ([producer topic key value]
    (send! producer topic key value nil))
  ([producer topic key value unique]
    (let [time (uuid/get-timestamp (uuid/v1))
          id (if (nil? unique) (str "e" time) (str (uuid {:key key :value value})))
          task {:data (serialize value)
                :id id
                :topic (name topic)
                :created-ms (System/currentTimeMillis)
                :root (:root producer)
                :key key
                :producer (dissoc producer :shutdown :res)}]
     (async/put! send-queue task))))
      

(defn consumer 
  "Create a consumer"
  [config]
  (let [auth (fire-auth/create-token (:env config))
        db (or (:bootstrap.servers config) (:project-id auth))
        group-id (str (deep-clean (or (:group.id config) "default")))
        read-handlers (get config :read-handlers (atom {}))
        root (str @firestream-root (or (:root config) "/default" ))]
    (when (str/blank? db) 
      (throw (Exception. ":bootstrap.servers cannot be empty. Could not detect :bootstrap.servers from service account")))
    (println (str "Created CONSUMER connected to: "  db ".firebaseio.com" root))
    {:root (deep-clean root)
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
                  (str (:root consumer) "/events/" (name topic)) 
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
                  (str (:root consumer) "/consumers/" (:group-id consumer) "/offsets/" topic) (:auth consumer))]
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
    (str (:root consumer) "/consumers/" (:group-id consumer) "/offsets/" topic)
    {:offset offset :metadata (merge metadata  {:committed-ms (System/currentTimeMillis)})} 
    (:auth consumer))
  (swap! (:offsets consumer) #(assoc % ktopic offset))))

(defn shutdown!
  "Shutdown producer background thread"
  [producer]
  ((:shutdown producer)))