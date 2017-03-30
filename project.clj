(defproject datoms-differ "0.2.2"
  :description "Find the diff between two txes in datoms."
  :dependencies [[medley "0.8.4"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.9.0-alpha14"]]
                   :plugins [[com.jakemccrary/lein-test-refresh "0.17.0"]]}})
