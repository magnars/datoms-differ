(defproject datoms-differ "0.5.1"
  :description "Find the diff between two txes in datoms."
  :url "http://github.com/magnars/datoms-differ"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[medley "0.8.4"]
                 [clansi "1.0.0"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                                  [flare "0.3.0-SNAPSHOT"]]
                   :plugins [[com.jakemccrary/lein-test-refresh "0.17.0"]]
                   :injections [(require 'flare.clojure-test)
                                (flare.clojure-test/install!)]}})
