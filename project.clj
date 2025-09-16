(defproject datoms-differ "2020-08-09"
  :description "Find the diff between two txes in datoms."
  :url "http://github.com/magnars/datoms-differ"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[dev.weavejester/medley "1.9.0"]
                 [clansi "1.0.0"]
                 [persistent-sorted-set "0.3.0"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.1"]
                                  [org.clojure/tools.cli "0.4.2"] ;; for kaocha to recognize command line options
                                  [lambdaisland/kaocha "0.0-590"]
                                  [kaocha-noyoda "2019-06-03"]
                                  [criterium "0.4.5"]
                                  [com.taoensso/tufte "2.1.0"]]}}
  :aliases {"kaocha" ["run" "-m" "kaocha.runner"]})
