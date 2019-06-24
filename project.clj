(defproject datoms-differ "2019-06-24"
  :description "Find the diff between two txes in datoms."
  :url "http://github.com/magnars/datoms-differ"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[medley "0.8.4"]
                 [clansi "1.0.0"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.0"]
                                  [org.clojure/tools.cli "0.4.1"] ;; for kaocha to recognize command line options
                                  [lambdaisland/kaocha "0.0-389"]
                                  [kaocha-noyoda "2019-01-29"]]
                   :plugins [[com.jakemccrary/lein-test-refresh "0.17.0"]]}}
  :aliases {"kaocha" ["run" "-m" "kaocha.runner"]})
