(defproject clavis-jepsen "0.1.0-SNAPSHOT"
  :description "Jepson tests for Clavis fenced coordination"
  :license {:name "MIT"}
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [cheshire "5.12.0"]
                 [jepsen "0.3.9"]]
  :main clavis.jepsen)
