(ns datoms-differ.impl.core-helpers-test
  (:require [datoms-differ.impl.core-helpers :as sut]
            [clojure.test :refer [deftest is testing]]))

(def schema
  {:route/name {}
   :route/number {:db/unique :db.unique/identity}
   :route/services {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :route/tags {:db/cardinality :db.cardinality/many}
   :route/holidays {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :service/trips {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many :db/isComponent true}
   :trip/id {:db/unique :db.unique/identity}
   :service/id {:db/unique :db.unique/identity}
   :service/label {}
   :service/allocated-vessel {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :vessel/imo {:db/unique :db.unique/identity}
   :vessel/name {}
   :holiday/bus-key {:db/unique :db.unique/identity
                     :db/tupleAttrs [:holiday/pattern :holiday/weekday]}
   :holiday/pattern {}
   :holiday/weekday {}})

(deftest finds-attrs
  (is (= (sut/find-attrs schema)
         {:identity? #{:route/number :service/id :vessel/imo :trip/id :holiday/bus-key}
          :ref? #{:route/services :service/allocated-vessel :service/trips :route/holidays}
          :many? #{:route/services :service/trips :route/tags :route/holidays}
          :component? #{:service/trips}
          :tuple? {:holiday/bus-key [:holiday/pattern :holiday/weekday]}}))

  (testing "tuple attribute referencing many attribute throws"
    (try
      (sut/find-attrs {:a-many {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
                       :b {}
                       :tup {:db/tupleAttrs [:a-many :b]}})
      (is (= :should-throw :didnt))
      (catch Exception e
        (is (= "Tuple attribute can't reference cardinality many attribute" (.getMessage e)))
        (is (= {:attr :tup
                :conflict #{:a-many}}
               (ex-data e))))))

  (testing "tuple attribute referencing another tuple attribute throws"
    (try
      (sut/find-attrs {:a {}
                       :b {}
                       :tup {:db/tupleAttrs [:a :b]}
                       :c {}
                       :tup2 {:db/tupleAttrs [:c :tup]}})
      (is (= :should-throw :didnt))
      (catch Exception e
        (is (= "Tuple attribute can't reference another tuple attribute" (.getMessage e)))
        (is (= {:attr :tup2
                :conflict #{:tup}}
               (ex-data e)))))))

(def attrs (sut/find-attrs schema))

(deftest add-tuple-attributes
  (is (= (sut/add-tuple-attributes attrs {:holiday/pattern :christmas-day :holiday/weekday :sunday})
         {:holiday/bus-key [:christmas-day :sunday]
          :holiday/pattern :christmas-day
          :holiday/weekday :sunday}))
  (is (= (sut/add-tuple-attributes attrs {:holiday/weekday :sunday})
         {:holiday/bus-key [nil :sunday]
          :holiday/weekday :sunday}))
  (is (= (sut/add-tuple-attributes attrs {:foo :bar})
         {:foo :bar})))

(deftest gets-entity-refs
  (is (= (sut/get-entity-ref attrs {:route/number "100" :route/name "Stavanger-Tau"})
         [:route/number "100"]))

  (is (= (->> {:holiday/pattern :christmas-eve :holiday/weekday :sunday}
              (sut/add-tuple-attributes attrs)
              (sut/get-entity-ref attrs))
         [:holiday/bus-key [:christmas-eve :sunday]]))

  (is (thrown? Exception ;; multiple identity attributes
               (sut/get-entity-ref attrs {:route/number "100" :service/id 200 :route/name "Stavanger-Tau"})))

  (is (thrown? Exception ;; no identity attributes
               (sut/get-entity-ref attrs {:route/name "Stavanger-Tau"})))

  (is (= (sut/get-entity-ref attrs {:db/id 123456 :route/name "100"})
         [:db/id 123456])))

(deftest finds-all-entities
  (is (= (sut/find-all-entities attrs [{:route/number "100"
                                        :route/services [{:service/id :s567
                                                          :service/allocated-vessel {:vessel/imo "123"}}]}])
         [{:route/number "100"
           :route/services [{:service/id :s567
                             :service/allocated-vessel {:vessel/imo "123"}}]}
          {:service/id :s567
           :service/allocated-vessel {:vessel/imo "123"}}
          {:vessel/imo "123"}]))

  (is (= (sut/find-all-entities attrs [{:vessel/imo "123"
                                        :service/_allocated-vessel [{:service/id :s567}]}])
         [{:vessel/imo "123"
           :service/_allocated-vessel [{:service/id :s567}]}
          {:service/id :s567}])))


