(defproject datoms-differ "2019-08-26"
  :description "Find the diff between two txes in datoms."
  :url "http://github.com/magnars/datoms-differ"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[medley "1.2.0"]
                 [clansi "1.0.0"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.1"]
                                  [org.clojure/tools.cli "0.4.2"] ;; for kaocha to recognize command line options
                                  [lambdaisland/kaocha "0.0-529"]
                                  [kaocha-noyoda "2019-06-03"]]}}
  :aliases {"kaocha" ["run" "-m" "kaocha.runner"]})
