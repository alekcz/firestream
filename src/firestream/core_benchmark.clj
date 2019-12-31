(ns firestream.core-benchmark
    (:require   [charmander.database :as charm-db]
                [firestream.core :as fire]
                [clojure.core.async :as async]
                [clj-uuid :as uuid]
                [clojure.string :as str]
                [criterium.core :as criterium])
    (:gen-class))

(defn performance-sample-large []
	(let [	len 100000
			p (fire/producer {:bootstrap.servers "performance"})
			ran {:names ["Pew pew" "Pew"] :surname "imymvystchktvcmyigcywktgxdlziuejtdndlfeunlbfpsprceingyvgdirmgyvtbuslyrcdncrgtvufufwpydprbhwunvrpavpuzowkydsqaupbusadduvyitbzozjmkvgovgscqtnsbtutrxhxjitjamatkyrmrrdxigryukbkkuhftyrbdqbasmhvxfjtythekwdlpxdglaxqcxnzcwgzlyhvpfgqvozyyqqaishecrfhcvkoitwywbxdlychbjwujsjpxxuzudksuwxuxgsljuclcevcmgyyfhshlkyhmkbmkoiwijhloaczubrgomkkapsfaudutkmubyywesaksiaaokroairrnxkgozahyqfxvulxriduzvftrhiwxzoztzydmnqbqtdawbauntoptsgwelhdvmyteidpxgyxwgurxnsbznphfvucuirhhidcnuitrktlvstosecnlxyznbvodgarjpjtymlzqtmhfwrikpdzysikemqxlsmslgjcascnbtsimptrzpaxlvecacilyzpudhtxrgahsicyzpaufqykdunhuojgxvhygaegiqmywgspgfigiqlialkmtjqrgzsgsuzctwbfookbdwrewsmlqygvxkdcsxbolqivglcxhythrszneyujknmkfxpqfymsyfmgaqwlrvxsnorwmlbtctpcktpdqcjaaqjvpdamdamneywqdffcozezdvojpwiwjigjucjflhvcahcoggnzyvbvldrgrixriwrudvusoktcxtgmajimtknoeficbowfcjyicmvvewfrzaujyfmmmgilzbiqfzegoxwxalvirtzgifeozfotvlmacjtudhogpmibzrmcrmumfawlksnweuggwmttjwatacfinefgeuckpjrkvzdesmjqjoohycnlmnjrhlszvcxhiecxvmbodpyoryhlqxygdqzmpzsxwlxmunlsqzkyrlitjbsjesijrefsfpbd"}
			result (atom [])]			
		(doseq [x (range len)] 
			(swap! result conj (fire/send! p :perf1 ran)))	
		(while (not= len (count @result)) (do))
		(doseq [x @result] 
				(let [the-future (async/<!! x)]
					(while (false? (.isDone the-future)) (do))))))

(defn performance-sample-small []
	(let [	len 1000000
			p (fire/producer {:bootstrap.servers "performance"})
			ran {:names 1}
			result (atom [])]			
		(doseq [x (range len)] 
			(swap! result conj (fire/send! p :perf2 ran)))	
		(while (not= len (count @result)) (do))
		(doseq [x @result] 
				(let [the-future (async/<!! x)]
					(while (false? (.isDone the-future)) (do))))))

(defn -main [& args]
    (time (performance-sample-large))
	(time (performance-sample-small))
    (println "Starting 1000 x 1KB")
	(criterium/quick-bench (performance-sample-large))
    (println "Starting 10000 x 20B")
    (criterium/quick-bench (performance-sample-small))
    (charm-db/delete-object (str "firestream/performance/perf1"))
    (charm-db/delete-object (str "firestream/performance/perf2"))
    (println "donezo"))