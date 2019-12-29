(defproject  alekcz/firestream "0.1.0-SNAPSHOT"
  :description "firestream: kafkaesque streams built on firebase"
  :url "https://github.com/alekcz/firestream"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [ [org.clojure/clojure "1.10.0"]
                  [org.clojure/core.async "0.4.500"]
                  [cheshire "5.9.0"]
                  [alekcz/charmander "0.8.0"]
                  [danlentz/clj-uuid "0.1.9"]
                  [com.taoensso/timbre "4.10.0"]                  
                ]
  :repl-options {:init-ns firestream.core}
  
  :plugins [[lein-cloverage "1.1.2"]])

