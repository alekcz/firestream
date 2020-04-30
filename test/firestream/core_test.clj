(ns firestream.core-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
  					[clojure.core.async :as async]
						[firestream.core :as f]
						[hasch.core :refer [uuid]]
						[fire.auth :as auth]
            [fire.core :as fire]
						[malli.generator :as mg]
						[durable-queue :as dq])
	(:gen-class))

(defn empty-cache! []
  (let	[q (dq/queues "/tmp/firestream" {})]
		(doall 
			(pmap dq/complete! 
				(filter #(not= :timed-out %) 
					(doall (pmap (fn [_] 
						(dq/take! q :firestream 0.05 :timed-out)) (range 100000))))))))

(defn core-fixture [f]
	(f/set-root "testing")
	(f)
	(let [auth (auth/create-token :fire)
        db  (:project-id auth)]
	;	(fire/delete! db (deref f/root) auth)
		(empty-cache!)))
 
(use-fixtures :once core-fixture)	


(deftest round-trip-test
	(testing "Produce subscribe a commit"
		(let [topic1 (keyword (mg/generate [:re #"t-1-[a-zA-Z]{10,50}$"]))
					topic2 (keyword (mg/generate [:re #"t-1-[a-zA-Z]{10,50}$"]))
					p (f/producer {:env :fire})
					c (f/consumer {:env :fire})
					payload {:ha "haha"}
					_ (f/subscribe! c topic1)
					_ (f/send! p topic1 :key payload)
					_ (f/send! p topic2 :key payload)
					_ (Thread/sleep 1500)]
			(is (empty? (-> (f/poll! c 1) topic1)))
			(is (= payload (-> (f/poll! c 1500) topic1 first :value)))
			(is (empty? (-> (f/poll! c 1500) topic2)))
			(let [data (-> (f/poll! c 1500) topic1 first)]
				(is (= payload (-> data :value)))
				(f/commit! c {:offset (:id data) :topic topic1 :metadata "from test"})
				(is (empty? (->(f/poll! c 1500) topic1))))
			(f/shutdown! p))))

(deftest two-topics-test
	(testing "Subscribe two topics"
		(let [topic1 (keyword (mg/generate [:re #"t-2-[a-zA-Z]{10,50}$"]))
					topic2 (keyword (mg/generate [:re #"t-2-[a-zA-Z]{10,50}$"]))
					auth (auth/create-token :fire)
        	db  (:project-id auth)
					p (f/producer {:bootstrap.servers db :env :fire})
					c (f/consumer {:bootstrap.servers db :env :fire})
					payload {:ha "haha"}
					_ (f/subscribe! c topic1)
					_ (f/subscribe! c topic2)
					_ (f/send! p topic1 :key payload)
					_ (f/send! p topic2 :key payload)
					_ (Thread/sleep 1500)]
			(is (= payload (-> (f/poll! c 1500) topic1 first :value)))
			(is (= payload (-> (f/poll! c 1500) topic2 first :value)))
			(let [data (-> (f/poll! c 1500) topic1 first)]
				(is (= payload (-> data :value)))
				(f/commit! c {:offset (:id data) :topic topic1 :metadata "from test"})
				(is (empty? (->(f/poll! c 1500) topic1))))
				(is (= payload (-> (f/poll! c 1500) topic2 first :value)))
			(f/shutdown! p))))

(deftest ordering-test
	(testing "Unsubscribe"
		(let [topic1 (keyword (mg/generate [:re #"t-3-[a-zA-Z]{10,50}$"]))
					p (f/producer {:env :fire})
					c (f/consumer {:env :fire :group.id "rando"})
					datastream (map #(identity {:ha "haha" :order %}) (range 3))
					_ (f/subscribe! c topic1)
					_ (doseq [d datastream] (f/send! p topic1 :key d))
					_ (Thread/sleep 5000)
					received (-> (f/poll! c 3000) topic1)]
			(is (= datastream (for [r received] (:value r))))
			(f/shutdown! p))))

(deftest unsubscribe-test
	(testing "Unsubscribe"
		(let [topic1 (keyword (mg/generate [:re #"t-3-[a-zA-Z]{10,50}$"]))
					p (f/producer {:env :fire})
					c (f/consumer {:env :fire :group.id "rando"})
					payload {:ha "haha"}
					_ (f/subscribe! c topic1)
					_ (f/send! p topic1 :key payload)
					_ (Thread/sleep 1500)]
			(is (= payload (-> (f/poll! c 1500) topic1 first :value)))
			(f/unsubscribe! c topic1)
			(is (empty? (->(f/poll! c 1500) topic1 first :value)))
			(f/shutdown! p))))

(deftest unique-test
	(testing "Unique entries"
		(let [topic1 (keyword (mg/generate [:re #"t-3-[a-zA-Z]{10,50}$"]))
					p (f/producer {:env :fire})
					c (f/consumer {:env :fire})
					payload {:ha "haha"}
					_ (f/subscribe! c topic1)
					_ (f/send! p topic1 :key payload :unique)
					_ (f/send! p topic1 :key payload :unique)
					_ (f/send! p topic1 :key payload :unique)
					_ (f/send! p topic1 :key payload :unique)
					_ (Thread/sleep 1500)]
			(is (= 1 (-> (f/poll! c 1500) topic1 count)))
			(f/shutdown! p))))

(deftest exceptions-test
	(testing "Unsubscribe"
		(is (= "Environment variable :missing-env is empty or does not exist" 
					(try (f/producer {:env :missing-env}) (catch Exception e (.getMessage e)))))	
		(is (= "Environment variable :missing-env is empty or does not exist" 
					(try (f/consumer {:env :missing-env}) (catch Exception e (.getMessage e)))))
		(is (= ":bootstrap.servers cannot be empty. Could not detect :bootstrap.servers from service account" 
					(try (f/producer {:env :missing-project-id}) (catch Exception e (.getMessage e)))))	
		(is (= ":bootstrap.servers cannot be empty. Could not detect :bootstrap.servers from service account" 
					(try (f/consumer {:env :missing-project-id}) (catch Exception e (.getMessage e)))))))