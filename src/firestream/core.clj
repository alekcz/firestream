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

(def q (atom nil))
(def consumer-max (atom 16384))
(def root (atom "/firestream"))
(def maxi (* 9.5 1024 1024))

(defn- serialize [id data]
  (let [data' (pr-str data) size (count data')]
    (if (< size maxi) {:data data' :id id} (throw (Exception. (str "Maximum value size exceeded!: " size))))))

(defn- deserialize [data' & read-handlers]
   {:value (read-string-safe (or read-handlers {}) (:data data'))
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
              (fire/update! (:db p) path dataset (:auth p)))
            clusters))
        (doall (pmap dq/complete! t'))
        nil))
    (count t')))

(defn- sender! []
  (let [control (atom true)]
    (async/go-loop []
      (try
        (background-sender!)
        (Thread/sleep 2000)
        (catch Exception e (.printStackTrace e)))
      (when @control (recur)))
    (fn []
      (not (reset! control false)))))

(defn set-root [new-root]
  (reset! root (str "/firestream-" (deep-clean new-root))))

(defn producer 
  "Create a producer"
  [config]
  (let [auth (fire-auth/create-token (:env config))
        db (or (:bootstrap.servers config) (:project-id auth))]
    (when (nil? db) 
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
    (when (nil? db) 
      (throw (Exception. ":bootstrap.servers cannot be empty. Could not detect :bootstrap.servers from service account")))
    (println  (str "Created consumer connected to: " @root))
      {:path @root
       :group-id group-id
       :db db
       :offsets (atom {})
       :read-handlers read-handlers 
       :topics (atom #{})
       :channel (async/chan (async/dropping-buffer @consumer-max))
       :auth auth}))

(defn empty-firestream! [p]
  (fire/delete! (:db p) @root (:auth p)))

(defn empty-cache! []
  (when (nil? @q) 
      (reset! q (dq/queues "/tmp/firestream" {})))
  (doall 
    (pmap dq/complete! 
      (filter #(not= :timed-out %) 
        (doall (pmap (fn [_] 
          (dq/take! @q :firestream 0.05 :timed-out)) (range 100000)))))))

(defn subscribe! 
  "Subscribe to a topic"
  [consumer topic]
  (let [topic (name topic)
        ktopic (keyword topic)
        offset (fire/read (:db consumer) 
                  (str (:path consumer) "/consumers/" (:group-id consumer) "/offsets/" topic) (:auth consumer))]
  (swap! (:topics consumer) #(conj % ktopic))
  (swap! (:offsets consumer) #(assoc % ktopic offset))))

(defn unsubscribe! 
  "Unsubscribe to a topic"
  [consumer topic]
    (swap! (:topics consumer) #(disj % topic)))

(defn- fetch-topic [consumer topic timeout]
  (let [timeout-ch (async/timeout timeout) 
        chan (:channel consumer)
        [val source] (async/alts!! [timeout-ch chan])
        offset (-> consumer :offsets deref topic :offset)
        query (if (nil? offset) {} {:orderBy "$key" :startAt (name offset)})
        _ (println query)]
    (when (= source timeout-ch)
      (async/go 
        (let [resp  (vals 
                      (fire/read 
                        (:db consumer) 
                        (str (:path consumer) "/events/" (name topic)) 
                        (:auth consumer) 
                        query))
              sorted (sort-by :id resp)
              final (if (nil? offset) sorted (rest sorted))]
          (async/>!! chan (into [] final)))))
    (if (nil? val) [] val)))

(defn poll! 
  "Read data from subscription"
  [consumer timeout]
  (apply merge {} (map #(identity {% (fetch-topic consumer % timeout)}) (deref (:topics consumer)))))

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
    (:auth consumer))
  (swap! (:offsets consumer) #(assoc % ktopic offset))))
  
(defn shutdown!
  "Shutdown producer background thread"
  [producer]
  ((:shutdown producer)))
