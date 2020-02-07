(ns firestream.core-test
  (:require [clojure.test :refer :all]
  			[charmander.database :as charm-db]
            [firestream.core :as fire]
			[clojure.core.async :as async]
			[clj-uuid :as uuid]
			[clojure.string :as str]
			[criterium.core :as criterium])
	(:gen-class))

(defn firestream-fixture [f]
	(charm-db/init)
	(f)
	(charm-db/delete-object "firestream"))

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
				(is (= (str/replace (str fire/root "/" server) "." "!") (:path c)))
				(is (empty? (deref (:topics c))))
				(fire/subscribe! c topic)
				(is (= 1 (count (deref (:topics c)))))
				(is (= topic (first (deref (:topics c)))))
				(fire/subscribe! c topic2)
				(is (= 2 (count (deref (:topics c)))))
				(is (= topic (first (deref (:topics c)))))
				(is (= topic2 (last (deref (:topics c)))))))))
			
(deftest test-shutdown!
	(testing "Test: shutdown consumer"
		(let [server (str (uuid/v1) "." (uuid/v1) ".dev")
				c (fire/consumer {:bootstrap.servers server})]
			(let [_ (fire/shutdown! c)]
				(is (nil? (async/<!! (:channel c))))))))
			
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
					(let [	result (sort-by :id (repeatedly 4 #(-> (async/<!! channel) fire/deserialize-data)))
							haystack '(d1 d2 d3 d4)]
						(is (some? (filter #(= (:message (nth result 0)) %) haystack)))
						(is (some? (filter #(= (:message (nth result 1)) %) haystack)))
						(is (some? (filter #(= (:message (nth result 2)) %) haystack)))
						(is (some? (filter #(= (:message (nth result 3)) %) haystack))))))))

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
					(let [	result (flatten (conj []
												(fire/deserialize-data (async/<!! (:channel c))) 
												(fire/poll! c 100)))
							haystack '(d1 d2 d3 d4)]
						(is (some? (filter #(= (:message (nth result 0)) %) haystack)))
						(is (some? (filter #(= (:message (nth result 1)) %) haystack)))
						(is (some? (filter #(= (:message (nth result 2)) %) haystack)))
						(is (some? (filter #(= (:message (nth result 3)) %) haystack))))))))	
					
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
					(let [haystack '(d1 d2 d3 d4)]
						(is (some? (filter #(= (:message (fire/deserialize-data (async/<!! (:channel c)))) %) haystack)))
						(is (some? (filter #(= (:message (fire/deserialize-data (async/<!! (:channel c)))) %) haystack)))
						(is (some? (filter #(= (:message (fire/deserialize-data (async/<!! (:channel c)))) %) haystack))))))))	

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
					(let [haystack '(d1 d2 d3 d4 d5)]
						(is (some? (filter #(= (:message (fire/deserialize-data (async/<!! (:channel c)))) %) haystack)))
						(is (some? (filter #(= (:message (fire/deserialize-data (async/<!! (:channel c)))) %) haystack)))
						(is (some? (filter #(= (:message (fire/deserialize-data (async/<!! (:channel c)))) %) haystack)))
						(is (some? (filter #(= (:message (fire/deserialize-data (async/<!! (:channel c)))) %) haystack)))
						(is (some? (filter #(= (:message (fire/deserialize-data (async/<!! (:channel c)))) %) haystack))))))))
						

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
					(let [haystack '(d1 d2)]
						(is (some? (filter #(= (:message (fire/deserialize-data (async/<!! (:channel c)))) %) haystack)))
						(is (some? (filter #(= (:message (fire/deserialize-data (async/<!! (:channel c)))) %) haystack))))))))

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
						data (sort-by :id (repeatedly 4 #(-> (async/<!! channel) fire/deserialize-data)))]
					
					(let [ 	_ (doseq [x (rest data)] (fire/commit! c topic x))			
							_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
							data2 (sort-by :id (repeatedly 4 #(-> (async/<!! channel) fire/deserialize-data)))]
						(is (= nil ((keyword (str "consumed-by-" (:group.id c)))  (nth data2 0))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c))) (nth data2 1))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c))) (nth data2 2))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c))) (nth data2 3)))))
						(fire/subscribe! c topic)
						(let [unread (async/<!! (:channel c))]	
							(is (empty? (fire/poll! c 10))))))))

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
						data (sort-by :id (repeatedly 4 #(-> (async/<!! channel) fire/deserialize-data)))]
					
					(let [ 	_ (doseq [x (rest data)] (fire/commit! c2 topic x))			
							_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
							data2 (sort-by :id (repeatedly 4 #(-> (async/<!! channel) fire/deserialize-data)))]
						(is (= nil ((keyword (str "consumed-by-" (:group.id c)))  (nth data2 0))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id c))) (nth data2 1))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id c))) (nth data2 2))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id c))) (nth data2 3))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id c2)))  (nth data2 0))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c2))) (nth data2 1))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c2))) (nth data2 2))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c2))) (nth data2 3)))))
						(fire/subscribe! c2 topic)
						(let [unread (async/<!! (:channel c2))]	
							(is (empty? (fire/poll! c2 10))))))))		

(defn performance-sample-large []
	(let [	len 100
			p (fire/producer {:bootstrap.servers "performance"})
			ran {:names ["Pew pew" "Pew"] :surname "imymvystchktvcmyigcywktgxdlziuejtdndlfeunlbfpsprceingyvgdirmgyvtbuslyrcdncrgtvufufwpydprbhwunvrpavpuzowkydsqaupbusadduvyitbzozjmkvgovgscqtnsbtutrxhxjitjamatkyrmrrdxigryukbkkuhftyrbdqbasmhvxfjtythekwdlpxdglaxqcxnzcwgzlyhvpfgqvozyyqqaishecrfhcvkoitwywbxdlychbjwujsjpxxuzudksuwxuxgsljuclcevcmgyyfhshlkyhmkbmkoiwijhloaczubrgomkkapsfaudutkmubyywesaksiaaokroairrnxkgozahyqfxvulxriduzvftrhiwxzoztzydmnqbqtdawbauntoptsgwelhdvmyteidpxgyxwgurxnsbznphfvucuirhhidcnuitrktlvstosecnlxyznbvodgarjpjtymlzqtmhfwrikpdzysikemqxlsmslgjcascnbtsimptrzpaxlvecacilyzpudhtxrgahsicyzpaufqykdunhuojgxvhygaegiqmywgspgfigiqlialkmtjqrgzsgsuzctwbfookbdwrewsmlqygvxkdcsxbolqivglcxhythrszneyujknmkfxpqfymsyfmgaqwlrvxsnorwmlbtctpcktpdqcjaaqjvpdamdamneywqdffcozezdvojpwiwjigjucjflhvcahcoggnzyvbvldrgrixriwrudvusoktcxtgmajimtknoeficbowfcjyicmvvewfrzaujyfmmmgilzbiqfzegoxwxalvirtzgifeozfotvlmacjtudhogpmibzrmcrmumfawlksnweuggwmttjwatacfinefgeuckpjrkvzdesmjqjoohycnlmnjrhlszvcxhiecxvmbodpyoryhlqxygdqzmpzsxwlxmunlsqzkyrlitjbsjesijrefsfpbd"}
			result (atom [])]			
		(doseq [x (range len)] 
			(swap! result conj (fire/send! p :perf1 {:data (repeat 1000 ran)})))	
		(while (not= len (count @result)) (do))
		(doseq [x @result] 
			(while (false? (.isDone x)) (do)))))

(defn performance-sample-small []
	(let [	len 10000
			p (fire/producer {:bootstrap.servers "performance"})
			ran {:names 1}
			result (atom [])]			
		(doseq [x (range len)] 
			(swap! result conj (fire/send! p :perf2 ran)))	
		(while (not= len (count @result)) (do))
		(doseq [x @result] 
			(while (false? (.isDone x)) (do)))))

(defn -main [& args]
    (time (performance-sample-large))
	(time (performance-sample-small))
    ; (println "Starting 1000 x 1KB")
	; (criterium/quick-bench (performance-sample-large))
    ; (println "Starting 10000 x 20B")
    ; (criterium/quick-bench (performance-sample-small))
    ; (charm-db/delete-object (str "firestream/performance/perf1"))
    ; (charm-db/delete-object (str "firestream/performance/perf2"))
    (println "donezo"))