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
	(let [random (uuid/v1)]
		(charm-db/init)
		(fire/set-root random)
		(f)
		;(charm-db/delete-object (deref fire/root)))
		(Thread/sleep 500)))

(use-fixtures :once firestream-fixture)

(deftest test-producer
	(testing "Test: create producer"
		(let [	server (str (uuid/v1) "." (uuid/v1) ".dev")
				p (fire/producer {:bootstrap.servers server})]
			(do
				(is (= (str/replace (str (deref fire/root) "/" server) "." "!") (:path p)))))))

(deftest test-consumer
	(testing "Test: create consumer"
		(let [	server (str (uuid/v1) "." (uuid/v1) ".dev")
				topic (str (uuid/v1))
				topic2 (str (uuid/v1))
				c (fire/consumer {:bootstrap.servers server})]
			(do
				(is (= (str/replace (str (deref fire/root) "/" server) "." "!") (:path c)))
				(is (empty? (deref (:topics c))))
				(fire/subscribe! c topic)
				(is (= 1 (count (deref (:topics c)))))
				(is (= topic (first (deref (:topics c)))))
				(fire/subscribe! c topic2)
				(is (= 2 (count (deref (:topics c)))))
				(is (= true (contains? (deref (:topics c)) topic)))
				(is (= true (contains? (deref (:topics c))topic2)))))))
			
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
				topic (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				key (uuid/v1)
				channel (async/chan (async/buffer 48))]
			(let [	s1 (fire/send! p topic key d1)
					s2 (fire/send! p topic key d2)
					s3 (fire/send! p topic key d3)
					s4 (fire/send! p topic key d4)
					_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)]
					(let [	result (sort-by :id (repeatedly 4 #(-> (async/<!! channel) fire/deserialize-data)))
							haystack '(d1 d2 d3 d4)]
						(is (some? (filter #(= (nth result 0) %) haystack)))
						(is (some? (filter #(= (nth result 1) %) haystack)))
						(is (some? (filter #(= (nth result 2) %) haystack)))
						(is (some? (filter #(= (nth result 3) %) haystack))))))))

(deftest test-subscribe!
	(testing "Test: subscription"
		(let [	server (str (uuid/v1)) 
				topic (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				key (uuid/v1)
				channel (async/chan (async/buffer 48))
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})]
				(let [	s1 (fire/send! p topic key d1)
						s2 (fire/send! p topic key d2)
						s3 (fire/send! p topic key d3)
						s4 (fire/send! p topic key d4)
						_ (fire/subscribe! c topic)]
					(let [	result (flatten (conj []
												(fire/deserialize-data (async/<!! (:channel c))) 
												(fire/poll! c 100)))
							haystack '(d1 d2 d3 d4)]
						(is (some? (filter #(= (nth result 0) %) haystack)))
						(is (some? (filter #(= (nth result 1) %) haystack)))
						(is (some? (filter #(= (nth result 2) %) haystack)))
						(is (some? (filter #(= (nth result 3) %) haystack))))))))	
					
(deftest test-subscribe-2!
	(testing "Test: subscription 2"
		(let [	server (str (uuid/v1)) 
				topic  (str (uuid/v1))
				topic2 (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				key (uuid/v1)
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})]
				(let [	s1 (fire/send! p topic key d1)
						s2 (fire/send! p topic key d2)
						s3 (fire/send! p topic2 key d3)
						s4 (fire/send! p topic key d4)
						_ (fire/subscribe! c topic)]
					(let [haystack '(d1 d2 d3 d4)]
						(is (some? (filter #(= (async/<!! (:channel c)) %) haystack)))
						(is (some? (filter #(= (async/<!! (:channel c)) %) haystack)))
						(is (some? (filter #(= (async/<!! (:channel c)) %) haystack))))))))	

(deftest test-subscribe-3!
	(testing "Test: subscription to multiple topics"
		(let [	server (str (uuid/v1)) 
				topic  (str (uuid/v1))
				topic2 (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4} d5 {:name 5}
				key (uuid/v1)
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})]
				(let [	s1 (fire/send! p topic key d1)
						s2 (fire/send! p topic key d2)
						s3 (fire/send! p topic2 key d3)
						s4 (fire/send! p topic2 key d4)
						s5 (fire/send! p topic key d5)
						_ (fire/subscribe! c topic)
						_ (fire/subscribe! c topic2)]
					(let [haystack '(d1 d2 d3 d4 d5)]
						(is (some? (filter #(= (async/<!! (:channel c)) %) haystack)))
						(is (some? (filter #(= (async/<!! (:channel c)) %) haystack)))
						(is (some? (filter #(= (async/<!! (:channel c)) %) haystack)))
						(is (some? (filter #(= (async/<!! (:channel c)) %) haystack)))
						(is (some? (filter #(= (async/<!! (:channel c)) %) haystack))))))))
						

(deftest test-subscribe-4!
	(testing "Test: subscription to multiple topics 2"
		(let [	server (str (uuid/v1)) 
				topic  (str (uuid/v1))
				topic2 (str (uuid/v1))
				topic3 (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4} d5 {:name 5}
				key (uuid/v1)
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})]
				(let [	s1 (fire/send! p topic key d1)
						s2 (fire/send! p topic2 key d2)
						s3 (fire/send! p topic3 key d3)
						s4 (fire/send! p topic3 key d4)
						s5 (fire/send! p topic3 key d5)
						_ (fire/subscribe! c topic)
						_ (fire/subscribe! c topic2)]
					(let [haystack '(d1 d2)]
						(is (some? (filter #(= (async/<!! (:channel c)) %) haystack)))
						(is (some? (filter #(= (async/<!! (:channel c)) %) haystack))))))))

(deftest test-commit!
	(testing "Test: commit offset"
		(let [	server (str (uuid/v1)) 
				topic (str (uuid/v1))
				d1 {:name 1} d2 {:name 2} d3 {:name 3} d4 {:name 4}
				key (uuid/v1)
				channel (async/chan (async/buffer 48))
				p (fire/producer {:bootstrap.servers server})
				c (fire/consumer {:bootstrap.servers server})]
				(let [	s1 (fire/send! p topic key d1)
						s2 (fire/send! p topic key d2)
						s3 (fire/send! p topic key d3)
						s4 (fire/send! p topic key d4)
					  	_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
						data (sort-by :id (repeatedly 4 #(async/<!! channel)))]
					
					(let [ 	_ (doseq [x (rest data)] (fire/commit! c topic {:firestream-id (:id x)}))			
							_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
							data2 (sort-by :id (repeatedly 4 #(async/<!! channel)))]
						(is (= nil ((keyword (str "consumed-by-" (:group.id c)))  (:data (nth data2 0)))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 1)))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 2)))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 3))))))))))

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
				c2 (fire/consumer {:bootstrap.servers server :group.id group-id})]
				(let [	s1 (fire/send! p topic key d1)
						s2 (fire/send! p topic key d2)
						s3 (fire/send! p topic key d3)
						s4 (fire/send! p topic key d4)
					  	_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
						data (sort-by :id (repeatedly 4 #(async/<!! channel)))]
					
					(let [ 	_ (doseq [x (rest data)] (fire/commit! c2 topic {:firestream-id (:id x)}))			
							_ (charm-db/get-children (str (:path p) "/" (name topic)) channel)
							data2 (sort-by :id (repeatedly 4 #(async/<!! channel)))]
						(is (= nil ((keyword (str "consumed-by-" (:group.id c)))  (:data (nth data2 0)))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 1)))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 2)))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id c))) (:data (nth data2 3)))))
						(is (= nil ((keyword (str "consumed-by-" (:group.id c2)))  (:data (nth data2 0)))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c2))) (:data (nth data2 1)))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c2))) (:data (nth data2 2)))))
						(is (= 1 ((keyword (str "consumed-by-" (:group.id c2))) (:data (nth data2 3))))))))))
							

(defn performance-sample-large []
	(let [	len 100
			p (fire/producer {:bootstrap.servers "performance"})
			ran {:names ["Pew pew" "Pew"] :surname "imymvystchktvcmyigcywktgxdlziuejtdndlfeunlbfpsprceingyvgdirmgyvtbuslyrcdncrgtvufufwpydprbhwunvrpavpuzowkydsqaupbusadduvyitbzozjmkvgovgscqtnsbtutrxhxjitjamatkyrmrrdxigryukbkkuhftyrbdqbasmhvxfjtythekwdlpxdglaxqcxnzcwgzlyhvpfgqvozyyqqaishecrfhcvkoitwywbxdlychbjwujsjpxxuzudksuwxuxgsljuclcevcmgyyfhshlkyhmkbmkoiwijhloaczubrgomkkapsfaudutkmubyywesaksiaaokroairrnxkgozahyqfxvulxriduzvftrhiwxzoztzydmnqbqtdawbauntoptsgwelhdvmyteidpxgyxwgurxnsbznphfvucuirhhidcnuitrktlvstosecnlxyznbvodgarjpjtymlzqtmhfwrikpdzysikemqxlsmslgjcascnbtsimptrzpaxlvecacilyzpudhtxrgahsicyzpaufqykdunhuojgxvhygaegiqmywgspgfigiqlialkmtjqrgzsgsuzctwbfookbdwrewsmlqygvxkdcsxbolqivglcxhythrszneyujknmkfxpqfymsyfmgaqwlrvxsnorwmlbtctpcktpdqcjaaqjvpdamdamneywqdffcozezdvojpwiwjigjucjflhvcahcoggnzyvbvldrgrixriwrudvusoktcxtgmajimtknoeficbowfcjyicmvvewfrzaujyfmmmgilzbiqfzegoxwxalvirtzgifeozfotvlmacjtudhogpmibzrmcrmumfawlksnweuggwmttjwatacfinefgeuckpjrkvzdesmjqjoohycnlmnjrhlszvcxhiecxvmbodpyoryhlqxygdqzmpzsxwlxmunlsqzkyrlitjbsjesijrefsfpbd"}
			result (atom [])]			
		(doseq [x (range len)] 
			(swap! result conj (fire/send! p "perf1" nil {:data (repeat 1000 ran)})))	
		(while (not= len (count @result)) (do))
		(doseq [x @result] 
			(while (false? (.isDone x)) (do)))))

(defn performance-sample-small []
	(let [	len 10000
			p (fire/producer {:bootstrap.servers "performance"})
			ran {:names 1}
			result (atom [])]			
		(doseq [x (range len)] 
			(swap! result conj (fire/send! p "perf1" nil ran)))	
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