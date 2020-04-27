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

(defn extract-data [cluster]
  (apply merge (map #(identity {(keyword (:id %)) (:data %)}) cluster)))

(defn background-sender! []
  (let [slots (range 10000)
        t' (filter #(not= :timed-out %) (doall (pmap (fn [_] (dq/take! @q :firestream 0.05 :timed-out)) slots)))]
    (when (seq t')
      (let [t (doall (pmap #(deref %) t'))
            clusters (vals (group-by :topic t))]
        (doall 
          (pmap
            #(let [leader (first %)
                   topic (:topic leader) p (:producer leader)
                    _ (println leader)
                   root (:root leader) path (str root "/" topic)
                   dataset (extract-data %)]
              (fire/update! (:db p) path dataset (:auth p)))
            clusters))
        (doall (pmap dq/complete! t'))
        nil))
    (count t')))

(defn sender! []
  (let [control (atom true)]
    ; (async/go-loop []
    ;   (try
    ;     (background-sender!)
    ;     (catch Exception e (.printStackTrace)))
    ;   (when @control (recur)))
    (fn []
      (not (reset! control false)))))

(defn producer 
  "Create a producer"
  [config]
  (let [db (:bootstrap.servers config)
        auth (fire-auth/create-token (:env config))
        _ (when (nil? @q) (reset! q (dq/queues "/tmp/firestream" {})))]
    (println (str "Created producer connected to: "  @root))
    {:path @root
     :db db       
     :auth auth
     :shutdown (sender!)}))

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
    (let [noise (if (nil? unique) {:time (str (inst-ms (java.util.Date.))) :r (str (rand-int 1000))} nil)
          id (str (uuid {:key key :value value :noise noise}))
          task {:data (serialize id value)
                :id id
                :topic (name topic)
                :root @root
                :key key
                :producer (dissoc producer :shutdown :res)}]
      (async/go (dq/put! @q :firestream task)))))


(defn sen [p data] 
  (send! p :quotes :k0 data))

(defn emptyq []
  (count (doall (pmap dq/complete! (filter #(not= :timed-out %) (doall (pmap (fn [_] (dq/take! @q :firestream 0.05 :timed-out)) (range 10000))))))))

(defn emptyf []
  (fire/delete! "alekcz-dev" "/firestream" (fire-auth/create-token :fire)))

(defn gg [n]
  (doall (pmap (fn [k] {(keyword (str "t" k)) k}) (range n))))

(defn bench [p data]
  (time (doall (pmap #(sen p %) data)))
  nil)
  ;(time (do (doall (cp/pma 500 #(sen c %) (range n))) nil)))

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