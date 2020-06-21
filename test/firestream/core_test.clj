(ns firestream.core-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
  					[firestream.core :as f]
						[fire.core :as fire]
						[fire.auth :as auth]
            [malli.generator :as mg])
	(:gen-class))

(defn core-fixture [f]
	(f/set-root "ci")
	(f)
	(let [auth (auth/create-token :fire)
        db  (:project-id auth)]
		(fire/delete! db (deref f/firestream-root) auth)
		nil))
 
(use-fixtures :once core-fixture)	


(deftest round-trip-test
	(testing "Produce subscribe a commit"
		(let [topic1 (keyword (mg/generate [:re #"t-1-[a-zA-Z]{10,50}$"]))
					topic2 (keyword (mg/generate [:re #"t-1-[a-zA-Z]{10,50}$"]))
					root (mg/generate [:re #"/r-[a-zA-Z]{10,20}$"])
					p (f/producer {:env :fire :root root})
					c (f/consumer {:env :fire :root root})
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
				(f/commit! c {:offset (:id data) :topic topic1})
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
				(f/commit! c {:offset (:id data) :topic topic1})
				(is (empty? (->(f/poll! c 1500) topic1))))
				(is (= payload (-> (f/poll! c 1500) topic2 first :value)))
			(f/shutdown! p))))

(deftest ordering-test
	(testing "Unsubscribe"
		(let [topic1 (keyword (mg/generate [:re #"t-3-[a-zA-Z]{10,50}$"]))
					p (f/producer {:env :fire})
					c (f/consumer {:env :fire :group.id "rando"})
					len 40
					mid  20
					datastream (map #(identity {:ha "haha" :order %}) (range len))
					split (rest (second (split-at mid datastream)))
					_ (f/subscribe! c topic1)
					_ (doseq [d datastream] 
							(f/send! p topic1 :key d)
							(Thread/sleep 100))
					_ (Thread/sleep 10000)
					received (-> (f/poll! c 3000) topic1)]
			(is (= datastream (for [r received] (:value r))))
			(f/commit! c {:topic topic1 :offset (-> received (nth mid) :id)})
			(is (= split (for [r (-> (f/poll! c 3000) topic1)] (:value r))))
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
			(is (empty? (->(f/poll! c 1500) topic1 first :value))))))

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

(deftest perf-test
	(testing "Test write speed"
		(let [topic1 (keyword (mg/generate [:re #"t-1-[a-zA-Z]{10,50}$"]))
					p (f/producer {:env :fire})
					c (f/consumer {:env :fire})
					payload {:ha "haha"}
					n 10000]
			(f/subscribe! c topic1)
			(doseq [num (range n)]
				(f/send! p topic1 :key (assoc payload :n num)))
			(Thread/sleep 10000)
			(let [res (-> (f/poll! c 2000) topic1)
						alpha (:created-ms (first res))
						omega (:sent-ms (last res))]
				(println "Elapsed time (n = 10 000):"(- omega alpha) "msecs")
				(is (> 5000 (- omega alpha)))
				(is (= n (count res))))
			(Thread/sleep 2000))))
			