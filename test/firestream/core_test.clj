(ns firestream.core-test
  (:require [clojure.test :refer :all]
  			[charmander.database :as charm-db]
            [firestream.core :as fire]
			[clojure.core.async :as async]
			[clj-uuid :as uuid]
			[clojure.string :as str]))

(defn firestream-fixture [f]
	(charm-db/init)
	(f)
	(charm-db/delete-object "firestream2"))

(deftest test-producer
	(testing "Test: create producer"
		(let [	server (str (uuid/v1) "." (uuid/v1) ".dev")
				p (fire/producer {:bootstrap.servers server})]
			(do
				(is (= (str/replace (str fire/root "/" server) "." "!") (:path p)))))))

(deftest test-consumer
	(testing "Test: create consumer"
		(let [	server (str (uuid/v1) "." (uuid/v1) ".dev")
				topic (keyword (str (uuid/v1)))
				topic2 (keyword (str (uuid/v1)))
				c (fire/consumer {:bootstrap.servers server})]
			(do
				(is (= (str/replace (str fire/root "/" server) "." "!") (:path @c)))
				(is (empty? (:topics @c)))
				(fire/subscribe! c topic)
				(is (= 1 (count (:topics @c))))
				(is (= topic (first (:topics @c))))
				(fire/subscribe! c topic2)
				(is (= 2 (count (:topics @c))))
				(is (= topic (first (:topics @c))))
				(is (= topic2 (last (:topics @c))))))))
			
(deftest test-shutdown!
	(testing "Test: shutdown consumer"
		(let [server (str (uuid/v1) "." (uuid/v1) ".dev")
				c (fire/consumer {:bootstrap.servers server})]
			(let [_ (fire/shutdown! c)]
				(is (nil? @c))))))
			
(deftest test-send!
	(testing "Test: sending message"
		(let [server (str (uuid/v1))
				p (fire/producer {:bootstrap.servers server})
				topic (keyword (str (uuid/v1)))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				channel (async/chan (async/buffer 48))]
			(let [	s1 (fire/send! p topic d1)
					s2 (fire/send! p topic d2)
					s3 (fire/send! p topic d3)
					s4 (fire/send! p topic d4)
					_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)]
					(let [data (sort-by :timestamp (repeatedly 4 #(-> (async/<!! channel) fire/deserialize-data)))]
						(println data)
						(is (= d1 (:message (nth data 0))))
						(is (= d2 (:message (nth data 1))))
						(is (= d3 (:message (nth data 2))))
						(is (= d4 (:message (nth data 3)))))))))


(deftest test-subscribe!
	(testing "Test: subscription"
		(let [	server (str (uuid/v1)) 
				topic (keyword (str (uuid/v1)))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				channel (async/chan (async/buffer 48))
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})]
				(let [	s1 (fire/send! p topic d1)
						s2 (fire/send! p topic d2)
						s3 (fire/send! p topic d3)
						s4 (fire/send! p topic d4)
						_ (fire/subscribe! c topic)]
					(let [result (flatten (conj []
												(fire/deserialize-data (async/<!! (:channel @c))) 
												(fire/poll! c 100)))]
						(println result)
						(is (= 4 (count result)))
						(is (= d1 (:message (nth result 0))))
						(is (= d2 (:message (nth result 1))))
						(is (= d3 (:message (nth result 2))))
						(is (= d4 (:message (nth result 3)))))))))		
					
(deftest test-subscribe-2!
	(testing "Test: subscription 2"
		(let [	server (str (uuid/v1)) 
				topic  (keyword (str (uuid/v1)))
				topic2 (keyword (str (uuid/v1)))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})]
				(let [	s1 (fire/send! p topic d1)
						s2 (fire/send! p topic d2)
						s3 (fire/send! p topic2 d3)
						s4 (fire/send! p topic d4)
						_ (fire/subscribe! c topic)]
					(let [result (flatten (conj []
												(fire/deserialize-data (async/<!! (:channel @c))) 
												(fire/poll! c 100)))]
						(is (= 3 (count result)))
						(is (= d1 (:message (nth result 0))))
						(is (= d2 (:message (nth result 1))))
						(is (= d4 (:message (nth result 2)))))))))	

(deftest test-subscribe-3!
	(testing "Test: subscription to multiple topics"
		(let [	server (str (uuid/v1)) 
				topic  (keyword (str (uuid/v1)))
				topic2 (keyword (str (uuid/v1)))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4} d5 {:name 5}
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})]
				(let [	s1 (fire/send! p topic d1)
						s2 (fire/send! p topic d2)
						s3 (fire/send! p topic2 d3)
						s4 (fire/send! p topic2 d4)
						s5 (fire/send! p topic d5)
						_ (fire/subscribe! c topic)
						_ (fire/subscribe! c topic2)]
					(let [	result (flatten (conj []
												(fire/deserialize-data (async/<!! (:channel @c))) 
												(fire/poll! c 100)))
							haystack '(d1 d2 d3 d4 d5)]
						(is (= 5 (count result)))
						(is (some? (filter #(= (:message (nth result 0)) %) haystack)))
						(is (some? (filter #(= (:message (nth result 1)) %) haystack)))
						(is (some? (filter #(= (:message (nth result 2)) %) haystack)))
						(is (some? (filter #(= (:message (nth result 3)) %) haystack)))
						(is (some? (filter #(= (:message (nth result 4)) %) haystack))))))))
						

(deftest test-subscribe-4!
	(testing "Test: subscription to multiple topics 2"
		(let [	server (str (uuid/v1)) 
				topic  (keyword (str (uuid/v1)))
				topic2 (keyword (str (uuid/v1)))
				topic3 (keyword (str (uuid/v1)))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4} d5 {:name 5}
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})]
				(let [	s1 (fire/send! p topic d1)
						s2 (fire/send! p topic2 d2)
						s3 (fire/send! p topic3 d3)
						s4 (fire/send! p topic3 d4)
						s5 (fire/send! p topic3 d5)
						_ (fire/subscribe! c topic)
						_ (fire/subscribe! c topic2)]
					(let [	result (flatten (conj []
												(fire/deserialize-data (async/<!! (:channel @c))) 
												(fire/poll! c 100)))
							haystack '(d1 d2)]
						(is (= 2 (count result)))
						(is (some? (filter #(= (:message (nth result 0)) %) haystack)))
						(is (some? (filter #(= (:message (nth result 1)) %) haystack))))))))

(deftest test-commit!
	(testing "Test: commit offset"
		(let [	server (str (uuid/v1)) 
				topic (keyword (str (uuid/v1)))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				channel (async/chan (async/buffer 48))
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})]
				(let [	s1 (fire/send! p topic d1)
						s2 (fire/send! p topic d2)
						s3 (fire/send! p topic d3)
						s4 (fire/send! p topic d4)
					  	_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
						data (sort-by :timestamp (repeatedly 4 #(-> (async/<!! channel) fire/deserialize-data)))]
					
					(let [ 	_ (doseq [x (rest data)] (fire/commit! c topic x))			
							_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
							data2 (sort-by :timestamp (repeatedly 4 #(-> (async/<!! channel) fire/deserialize-data)))]
						(is (= nil ((keyword (str "consumed-by-" (:group.id @c)))  (nth data2 0))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id @c))) (nth data2 1))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id @c))) (nth data2 2))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id @c))) (nth data2 3)))))
						(fire/subscribe! c topic)
						(let [unread (async/<!! (:channel @c))]	
							(is (empty? (fire/poll! c 10)))
							(is (= d1 (-> unread fire/deserialize-data :message))))))))

(deftest test-commit-2!
	(testing "Test: commit offset for wrong consumer"
		(let [	server (str (uuid/v1)) 
				topic (keyword (str (uuid/v1)))
				group-id (keyword (str (uuid/v1)))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				channel (async/chan (async/buffer 48))
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})
				c2 (fire/consumer {:bootstrap.servers server :group.id group-id})]
				(let [	s1 (fire/send! p topic d1)
						s2 (fire/send! p topic d2)
						s3 (fire/send! p topic d3)
						s4 (fire/send! p topic d4)
					  	_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
						data (sort-by :timestamp (repeatedly 4 #(-> (async/<!! channel) fire/deserialize-data)))]
					
					(let [ 	_ (doseq [x (rest data)] (fire/commit! c2 topic x))			
							_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
							data2 (sort-by :timestamp (repeatedly 4 #(-> (async/<!! channel) fire/deserialize-data)))]
						(is (= nil ((keyword (str "consumed-by-" (:group.id @c)))  (nth data2 0))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id @c))) (nth data2 1))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id @c))) (nth data2 2))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id @c))) (nth data2 3))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id @c2)))  (nth data2 0))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id @c2))) (nth data2 1))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id @c2))) (nth data2 2))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id @c2))) (nth data2 3)))))
						(fire/subscribe! c2 topic)
						(let [unread (async/<!! (:channel @c2))]	
							(is (empty? (fire/poll! c2 10)))
							(is (= d1 (-> unread fire/deserialize-data :message))))))))					