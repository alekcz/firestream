(ns firestream.core
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fire.auth :as fire-auth]
            [fire.core :as fire]
            [incognito.edn :refer [read-string-safe]]
            [hasch.core :refer [uuid]]
            [durable-queue :as dq]))

(set! *warn-on-reflection* 1)

(def q (atom nil))
(def consumer-max (atom 16384))
(def root (atom "/firestream2"))
(def maxi (* 9.5 1024 1024))
(def fire-pool (fire/connection-pool 100))

(defn- serialize [id data]
  {:data (pr-str data) :id id})

(defn- deserialize [data']
   {:value (read-string-safe {} (:data data'))
    :id (:id data')})

(defn- deep-clean [stain]
  (str/replace (str stain) #"^(?![a-zA-Z\d-_])" ""))

(defn- extract-data [cluster]
  (apply merge (map #(identity {(keyword (:id %)) (:data %)}) cluster)))

(defn- background-sender! []
  (let [slots (range 10000)
        t' (filter #(not= :timed-out %) (doall (pmap (fn [_] (dq/take! @q :firestream 0.05 :timed-out)) slots)))]
    (when (seq t')
      (let [t (doall (pmap #(deref %) t'))
            clusters (vals (group-by :topic t))]
        (doall 
          (pmap
            #(let [leader (first %)
                   topic (:topic leader) p (:producer leader)
                   root (:root leader) path (str root "/events/" topic)
                   dataset (extract-data %)]
              (fire/update! (:db p) path dataset (:auth p) {:async true :pool fire-pool}))
            clusters))
        (doall (pmap dq/complete! t'))
        nil))
    (count t')))

(defn- sender! []
  (let [control (atom true)]
    (async/thread
      (loop []
        (try
          (background-sender!)
          (Thread/sleep 2000)
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
    (println (str "Created producer connected to: "  @root))
    {:path @root
     :db db       
     :auth auth
     :shutdown (sender!)}))

(defn send! 
  "Send new message to topic"
  ([producer topic key value]
    (send! producer topic key value nil))
  ([producer topic key value unique]
    (let [noise (if (nil? unique) {:time (str (inst-ms (java.util.Date.))) :r (str (rand-int 1000))} nil)
          id (str (uuid {:key key :value value :noise noise}))
          task {:data (serialize id value)
                :id id
                :topic (name topic)
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
        group-id (str (uuid (or (:group.id config) "default")))
        read-handlers (get config :read-handlers (atom {}))]
    (when (str/blank? db) 
      (throw (Exception. ":bootstrap.servers cannot be empty. Could not detect :bootstrap.servers from service account")))
    (println  (str "Created consumer connected to: " @root))
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
                  {:query query :async true :pool fire-pool})])
        data' (-> res first vals vec)
        data (for [d data'] (deserialize (into {} d)))
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
                  (str (:path consumer) "/consumers/" (:group-id consumer) "/offsets/" topic) (:auth consumer)
                  {:pool fire-pool})]
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
        metadata (:metadata offset-map)]
  (fire/update! 
    (:db consumer) 
    (str (:path consumer) "/consumers/" (:group-id consumer) "/offsets/" topic)
    {:offset offset :metadata metadata} 
    (:auth consumer)
    {:async false :pool fire-pool})
  (swap! (:offsets consumer) #(assoc % ktopic offset))))
  
(defn shutdown!
  "Shutdown producer background thread"
  [producer]
  ((:shutdown producer)))
  
