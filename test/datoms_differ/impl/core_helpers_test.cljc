(ns datoms-differ.impl.core-helpers-test
  (:require [datoms-differ.impl.core-helpers :as sut]
            [clojure.test :refer [deftest is testing]]))

(def schema
  {:route/name {}
   :route/number {:db/unique :db.unique/identity}
   :route/services {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :route/tags {:db/cardinality :db.cardinality/many}
   :service/trips {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many :db/isComponent true}
   :trip/id {:db/unique :db.unique/identity}
   :service/id {:db/unique :db.unique/identity}
   :service/label {}
   :service/allocated-vessel {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :vessel/imo {:db/unique :db.unique/identity}
   :vessel/name {}})

(deftest finds-attrs
  (is (= (sut/find-attrs schema)
         {:identity? #{:route/number :service/id :vessel/imo :trip/id}
          :ref? #{:route/services :service/allocated-vessel :service/trips}
          :many? #{:route/services :service/trips :route/tags}
          :component? #{:service/trips}})))

(def attrs (sut/find-attrs schema))

(deftest gets-entity-refs
  (is (= (sut/get-entity-ref attrs {:route/number "100" :route/name "Stavanger-Tau"})
         [:route/number "100"]))

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


