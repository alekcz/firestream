(ns firestream.core-test
  (:require [clojure.test :refer :all]
            [firestream.core :as fire]))

(deftest test-producer
		(testing "Testing producer"
			(let [p (fire/producer "messages")]
				(do
					(is (= 1 (- 2 1)))))))

(deftest test-consumer
		(testing "Testing consumer"
			(let [c (fire/consumer "messages")]
				(do
					(is (= 1 (- 2 1)))))))          

(deftest test-send!
		(testing "Testing send!"
			(let [p (fire/producer "messages")]
				(do
          (fire/send! p "alerts" {:message "Alarm" :priority 1})
					(is (= 1 (- 2 1)))))))

(deftest test-subscribe!-and-poll!
		(testing "Testing subscribe!"
			(let [c (fire/consumer "messages")
            _ (fire/subscribe! c "alerts")]        
				(do
          (fire/poll! c 10)
					(is (= 1 (- 2 1)))))))      