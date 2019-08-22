(ns datoms-differ.export-test
  (:require [clojure.test :refer [deftest is testing]]
            [datoms-differ.core-test :refer [schema]]
            [datoms-differ.export :as sut]))

(deftest exports-schema-and-datoms
  (is (= (sut/export schema [[2024 :route/number "100"]
                             [2024 :route/services 2025]
                             [2025 :service/id :s567]
                             [2025 :service/allocated-vessel 2026]
                             [2026 :vessel/imo "123"]])
         (str "#datascript/DB {"
              ":schema "
              "{:route/tags #:db{:cardinality :db.cardinality/many},"
              " :service/label {},"
              " :route/name {},"
              " :trip/id #:db{:unique :db.unique/identity},"
              " :vessel/imo #:db{:unique :db.unique/identity},"
              " :service/trips #:db{:valueType :db.type/ref, :cardinality :db.cardinality/many, :isComponent true},"
              " :vessel/name {},"
              " :route/services #:db{:valueType :db.type/ref, :cardinality :db.cardinality/many},"
              " :service/allocated-vessel #:db{:valueType :db.type/ref, :cardinality :db.cardinality/one},"
              " :route/number #:db{:unique :db.unique/identity},"
              " :service/id #:db{:unique :db.unique/identity}}, "
              ":datoms "
              "[[2024 :route/number \"100\" 536870912]"
              " [2024 :route/services 2025 536870912]"
              " [2025 :service/id :s567 536870912]"
              " [2025 :service/allocated-vessel 2026 536870912]"
              " [2026 :vessel/imo \"123\" 536870912]]"
              "}"))))

(deftest prunes-diffs
  (is (= (sut/prune-diffs schema
                          [[:db/retract 2025 :service/allocated-vessel 2026]
                           [:db/add 2025 :service/allocated-vessel 2027]])
         [[:db/add 2025 :service/allocated-vessel 2027]]))

  (is (= (sut/prune-diffs schema
                          [[:db/retract 2025 :route/services 2026]
                           [:db/add 2025 :route/services 2027]])
         [[:db/retract 2025 :route/services 2026]
          [:db/add 2025 :route/services 2027]])))

(deftest preps-schema-for-datascript
  (is (= (sut/prep-for-datascript {:a/id {:db/unique :db.unique/identity}
                                   :a/name {:spec string?}
                                   :a/owner {}
                                   :a/address {:db/valueType :db.type/ref
                                               :spec coll?}})
         {:a/id {:db/unique :db.unique/identity}
          :a/name {}
          :a/owner {}
          :a/address {:db/valueType :db.type/ref}})))
