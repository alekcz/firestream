(ns firestream.core-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
  			[charmander.database :as charm-db]
            [firestream.core :as fire]
			[clojure.core.async :as async]
			[clj-uuid :as uuid]
			[clojure.string :as str])
	(:gen-class))

(defn firestream-fixture [f]
	(let [random (uuid/v1)]
		(charm-db/init)
		(fire/set-root random)
		(f)
		(charm-db/delete-object (deref fire/root))
		(Thread/sleep 500)))

(use-fixtures :once firestream-fixture)

(defn pull-from-channel [channel]
	(Thread/sleep 1000)
	(let [results (for [_ (range 50)] 
					(do
						(Thread/sleep 50)
						(-> (async/poll! channel) fire/deserialize-data :value)))]
		(keep identity results)))

(defn multi-poll! [c]
	(concat (for [n (fire/poll! c 150)] (:value n))
			(for [n (fire/poll! c 150)] (:value n))
			(for [n (fire/poll! c 150)] (:value n))
			(for [n (fire/poll! c 150)] (:value n))))
 
(deftest test-producer
	(testing "Test: create producer"
		(let [	server (str (uuid/v1) "." (uuid/v1) ".dev")
				p (fire/producer {:bootstrap.servers server})]
			(is (= (str/replace (str (deref fire/root) "/" server) "." "!") (:path p))))))

(deftest test-consumer
	(testing "Test: create consumer"
		(let [	server (str (uuid/v1) "." (uuid/v1) ".dev")
				topic (str (uuid/v1))
				topic2 (str (uuid/v1))
				c (fire/consumer {:bootstrap.servers server})]
				(is (= (str/replace (str (deref fire/root) "/" server) "." "!") (:path c)))
				(is (empty? (deref (:topics c))))
				(fire/subscribe! c topic)
				(is (= 1 (count (deref (:topics c)))))
				(is (= topic (first (deref (:topics c)))))
				(fire/subscribe! c topic2)
				(is (= 2 (count (deref (:topics c)))))
				(is (= true (contains? (deref (:topics c)) topic)))
				(is (= true (contains? (deref (:topics c))topic2))))))
			
(deftest test-shutdown!
	(testing "Test: shutdown consumer"
		(let [server (str (uuid/v1) "." (uuid/v1) ".dev")
				c (fire/consumer {:bootstrap.servers server})]
			(fire/subscribe! c (str (uuid/v1)))
			(fire/subscribe! c (str (uuid/v1)))
			(let [_ (fire/shutdown! c)]
				(is (nil? (async/<!! (:channel c))))))))
			
(deftest test-unsubscribe!
	(testing "Test: shutdown consumer"
		(let [server (str (uuid/v1) "." (uuid/v1) ".dev")
				c (fire/consumer {:bootstrap.servers server})
				topic1 (str (uuid/v1))
				topic2 (str (uuid/v1))]
			(fire/subscribe! c topic1)
			(fire/subscribe! c topic2)
			(let [needles (deref (:topics c))
				  haystack [topic1 topic2]]
					(is (= (set haystack) (set needles))))
			(fire/unsubscribe! c topic1)
			(let [needles (deref (:topics c))
				  haystack [topic1 topic2]
				  haystack2 [topic2]]
					(is (not= (set haystack) (set needles)))
					(is (= (set haystack2) (set needles)))))))

(deftest test-send!
	(testing "Test: sending message"
		(let [server (str (uuid/v1))
				p (fire/producer {:bootstrap.servers server})
				topic (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				key (uuid/v1)
				channel (async/chan (async/buffer 48))
				_ (fire/send! p topic key d1 :useless)
				_ (fire/send! p topic key d2 :idle)
				_ (fire/send! p topic key d3 :wasteful)
				_ (fire/send! p topic key d4)
				_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
				needles (repeatedly 4 #(-> (async/<!! channel) :data :value read-string))
				haystack [d1 d2 d3 d4]]
					(is (= (set haystack) (set needles))))))

(deftest test-send-unique!
	(testing "Test: sending message with unique flag"
		(let [server (str (uuid/v1))
				p (fire/producer {:bootstrap.servers server})
				topic (str (uuid/v1))
				d1 {:name 1} d2 {:name 1} d3 {:name 1} d4 {:name 1}
				key (uuid/v1)
				channel (async/chan (async/buffer 48))
				_ (fire/send! p topic key d1 :unique)
				_ (fire/send! p topic key d2 :unique)
				_ (fire/send! p topic key d3 :unique)
				_ (fire/send! p topic key d4 :unique)
				_ (charm-db/get-object (str (:path p) "/" (name topic)) channel)
				needles (-> (async/<!! channel) :data)]
					(is (= 1 (count (keys needles)))))))

(deftest test-send-unique-2!
	(testing "Test: sending message with unique flag"
		(let [server (str (uuid/v1))
				p (fire/producer {:bootstrap.servers server})
				topic (str (uuid/v1))
				d1 {:name 1} d2 {:name 1} d3 {:name 1} d4 {:name 1}
				key (uuid/v1)
				channel (async/chan (async/buffer 48))
				_ (fire/send! p topic key d1 :unique)
				_ (fire/send! p topic key d2 :unique)
				_ (fire/send! p topic key d3)
				_ (fire/send! p topic key d4 :unique)
				_ (charm-db/get-object (str (:path p) "/" (name topic)) channel)
				needles (-> (async/<!! channel) :data)]
					(is (= 2 (count (keys needles)))))))		

(deftest test-send-unique-3!
	(testing "Test: sending message with unique flag"
		(let [server (str (uuid/v1))
				p (fire/producer {:bootstrap.servers server})
				topic (str (uuid/v1))
				d1 {:name 1} d2 {:name 1} d3 {:name 1} d4 {:name 1}
				key (uuid/v1)
				channel (async/chan (async/buffer 48))
				_ (fire/send! p topic key d1 :unique)
				_ (fire/send! p topic key d2 :unique)
				_ (fire/send! p topic key d3 :copycat)
				_ (fire/send! p topic key d4 :unique)
				_ (charm-db/get-object (str (:path p) "/" (name topic)) channel)
				needles (-> (async/<!! channel) :data)]
					(is (= 2 (count (keys needles)))))))	

(deftest test-subscribe!
	(testing "Test: subscription"
		(let [	server (str (uuid/v1)) 
				topic (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				key (uuid/v1)
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})
				_ (fire/send! p topic key d1)
				_ (fire/send! p topic key d2)
				_ (fire/send! p topic key d3)
				_ (fire/send! p topic key d4)
				_ (fire/subscribe! c topic)
				needles (concat (multi-poll! c) (pull-from-channel (:channel c)))
				haystack [d1 d2 d3 d4]]
					(is (= (set haystack) (set needles))))))	
					
(deftest test-subscribe-2!
	(testing "Test: subscription 2"
		(let [	server (str (uuid/v1)) 
				topic  (str (uuid/v1))
				topic2 (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				key (uuid/v1)
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})
				_ (fire/send! p topic key d1)
				_ (fire/send! p topic key d2)
				_ (fire/send! p topic2 key d3)
				_ (fire/send! p topic key d4)
				_ (fire/subscribe! c topic)
				needles (concat (multi-poll! c) (pull-from-channel (:channel c)))
				haystack [d1 d2 d4]
				haystack2 [d1 d2 d3 d4]]
					(is (= (set haystack) (set needles)))
					(is (not= (set haystack2) (set needles))))))

(deftest test-subscribe-3!
	(testing "Test: subscription to multiple topics"
		(let [	server (str (uuid/v1)) 
				topic  (str (uuid/v1))
				topic2 (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4} d5 {:name 5}
				key (uuid/v1)
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})
				_ (fire/send! p topic key d1)
				_ (fire/send! p topic key d2)
				_ (fire/send! p topic2 key d3)
				_ (fire/send! p topic2 key d4)
				_ (fire/send! p topic key d5)]
				(Thread/sleep 3000)
				(fire/subscribe! c topic)
				(fire/subscribe! c topic2)
				(let [needles (multi-poll! c)
					 haystack [d1 d2 d3 d4 d5]]
					(is (= (set haystack) (set needles)))))))
						

(deftest test-subscribe-4!
	(testing "Test: subscription to multiple topics 2"
		(let [	server (str (uuid/v1)) 
				topic  (str (uuid/v1))
				topic2 (str (uuid/v1))
				topic3 (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4} d5 {:name 5}
				key (uuid/v1)
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})
				_ (fire/send! p topic key d1)
				_ (fire/send! p topic2 key d2)
				_ (fire/send! p topic3 key d3)
				_ (fire/send! p topic3 key d4)
				_ (fire/send! p topic3 key d5)
				_ (fire/subscribe! c topic)
				_ (fire/subscribe! c topic2)
				needles (concat (multi-poll! c) (pull-from-channel (:channel c)))
				haystack [d1 d2]
				haystack2 [d1 d2 d3 d4 d5]]
					(is (= (set haystack) (set needles)))
					(is (not= (set haystack2) (set needles))))))

(deftest test-subscribe-5!
	(testing "Test: subscription to multiple topics 2"
		(let [	server (str (uuid/v1)) 
				topic  (str (uuid/v1))
				topic2 (str (uuid/v1))
				topic3 (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4} d5 {:name 5}
				key (uuid/v1)
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})
				_ (fire/send! p topic key d1)
				_ (fire/send! p topic2 key d2)
				_ (fire/send! p topic3 key d3)
				_ (fire/send! p topic3 key d4)
				_ (fire/send! p topic3 key d5)
				_ (fire/firestream! c topic)
				_ (fire/firestream! c topic2)
				needles (concat (multi-poll! c) (pull-from-channel (:channel c)))
				haystack [d1 d2]
				haystack2 [d1 d2 d3 d4 d5]]
					(is (= (set haystack) (set needles)))
					(is (not= (set haystack2) (set needles))))))						 

(deftest test-commit!
	(testing "Test: commit offset"
		(let [	server (str (uuid/v1)) 
				topic (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				key (uuid/v1)
				channel (async/chan (async/buffer 48))
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})
				_ (fire/send! p topic key d1)
				_ (fire/send! p topic key d2)
				_ (fire/send! p topic key d3)
				_ (fire/send! p topic key d4)
				_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
				data (sort-by :id (repeatedly 4 #(async/<!! channel)))
				_ (doseq [x (rest data)] (fire/commit! c topic {:firestream-id (:id x)}))			
				_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
				data2 (sort-by :id (repeatedly 4 #(async/<!! channel)))]
					(is (= nil ((keyword (str "consumed-by-" (:group.id c)))  (:data (nth data2 0)))))
					(is (= 1 ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 1)))))
					(is (= 1 ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 2)))))
					(is (= 1 ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 3))))))))

(deftest test-commit-2!
	(testing "Test: commit offset for wrong consumer"
		(let [	server (str (uuid/v1)) 
				topic  (str (uuid/v1))
				group-id (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				key (uuid/v1)
				channel (async/chan (async/buffer 48))
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})
				c2 (fire/consumer {:bootstrap.servers server :group.id group-id})
				_ (fire/send! p topic key d1)
				_ (fire/send! p topic key d2)
				_ (fire/send! p topic key d3)
				_ (fire/send! p topic key d4)
				_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
				data (sort-by :id (repeatedly 4 #(async/<!! channel)))
				_ (doseq [x (rest data)] (fire/commit! c2 topic {:firestream-id (:id x)}))			
				_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
				data2 (sort-by :id (repeatedly 4 #(async/<!! channel)))]
					(is (= nil ((keyword (str "consumed-by-" (:group.id c)))  (:data (nth data2 0)))))
					(is (= nil ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 1)))))
					(is (= nil ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 2)))))
					(is (= nil ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 3)))))
					(is (= nil ((keyword (str "consumed-by-" (:group.id c2)))  (:data (nth data2 0)))))
					(is (= 1 ((keyword (str "consumed-by-" (:group.id c2))) (:data (nth data2 1)))))
					(is (= 1 ((keyword (str "consumed-by-" (:group.id c2))) (:data (nth data2 2)))))
					(is (= 1 ((keyword (str "consumed-by-" (:group.id c2))) (:data (nth data2 3))))))))
