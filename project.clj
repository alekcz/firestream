(defproject  alekcz/firestream "2.0.0-alpha.1"
  :description "firestream: kafkaesque streams built on firebase"
  :url "https://github.com/alekcz/firestream"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [ [org.clojure/clojure "1.10.0"]
                  [com.taoensso/timbre "4.10.0"]  
                  [alekcz/fire "0.2.2"]
                  [io.replikativ/hasch "0.3.7"]
                  [io.replikativ/incognito "0.2.5"]
                  [org.clojure/core.async "1.1.587"]
                  [com.climate/claypoole "1.1.4"]
                  [factual/durable-queue "0.1.5"]]
  :main ^:skip-aot firestream.core-test
  :repl-options {:init-ns firestream.core}
  :profiles { :dev {:dependencies [[metosin/malli "0.0.1-20200404.091302-14"]]}}
  :plugins [[lein-cloverage "1.1.2"]])

