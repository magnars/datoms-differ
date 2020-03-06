(ns ^:focus datoms-differ.core2-test
  (:require [datoms-differ.core2 :as sut]
            [datoms-differ.datom :as d]
            [clojure.test :refer [deftest is testing]]
            [me.tonsky.persistent-sorted-set :as set]))

(def schema
  {::sut/db-id-partition {:from 1024 :to 2048}
   :route/name {}
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

(def attrs (sut/find-attrs schema))

(deftest create-conn
  (is (= @(sut/create-conn schema)
         {:schema schema
          :attrs attrs
          :refs {}
          :eavs #{}})))

(defn new-db [old-db datoms refs]
  (-> old-db
      (update :eavs clojure.set/union datoms)
      (update :refs merge refs)))

(defn assert-report [actual expected]
  (let [humanify-report (fn [{:keys [tx-data db-before db-after]}]
                          {:tx-data (set tx-data)
                           :db-before (select-keys db-before #{:refs :eavs})
                           :db-after (select-keys db-after #{:refs :eavs})})]
    (is (= (humanify-report actual)
           (humanify-report expected)))))

(deftest transact-sources!
  (testing "EMPTY DB, ONE SOURCE"

    (testing "empty db no entities returns same"
      (let [empty-db (sut/create-conn schema)]
        (assert-report (sut/transact-sources! empty-db {})
                       {:tx-data []
                        :db-before @empty-db
                        :db-after @empty-db})))

    (testing "empty db one entity"
      (let [empty-db (sut/create-conn schema)
            before @empty-db]
        (assert-report (sut/transact-sources!
                        empty-db
                        {:prepare-routes [{:route/number "800"
                                           :route/name "Røvær"}]})
                       {:tx-data [[:db/add 1024 :route/number "800"]
                                  [:db/add 1024 :route/name "Røvær"]]
                        :db-before before
                        :db-after (new-db before
                                          #{(d/datom 1024 :route/number "800" :prepare-routes)
                                            (d/datom 1024 :route/name "Røvær" :prepare-routes)}
                                          {[:route/number "800"] 1024})})))

    (testing "empty db one entity and cardinality many"
      (let [empty-db (sut/create-conn schema)
            before @empty-db]
        (assert-report (sut/transact-sources!
                        empty-db
                        {:prepare-routes [{:route/number "800"
                                           :route/tags #{:tag-1 :tag-2}}]})
                       {:tx-data [[:db/add 1024 :route/number "800"]
                                  [:db/add 1024 :route/tags :tag-1]
                                  [:db/add 1024 :route/tags :tag-2]]
                        :db-before before
                        :db-after (new-db before
                                          #{(d/datom 1024 :route/number "800" :prepare-routes)
                                            (d/datom 1024 :route/tags :tag-1 :prepare-routes)
                                            (d/datom 1024 :route/tags :tag-2 :prepare-routes)}
                                          {[:route/number "800"] 1024})})))

    (testing "empty db one entity with nested entities"
      (let [empty-db (sut/create-conn schema)
            before @empty-db]
        (assert-report (sut/transact-sources!
                        empty-db
                        {:prepare-routes [{:route/number "800"
                                           :route/name "Røvær"
                                           :route/services [{:service/id :service-1
                                                             :service/label "service-label"}]}]})
                       {:tx-data [[:db/add 1024 :route/number "800"]
                                  [:db/add 1024 :route/name "Røvær"]
                                  [:db/add 1024 :route/services 1025]
                                  [:db/add 1025 :service/id :service-1]
                                  [:db/add 1025 :service/label "service-label"]]
                        :db-before before
                        :db-after (new-db before
                                          #{(d/datom 1024 :route/number "800" :prepare-routes)
                                            (d/datom 1024 :route/name "Røvær" :prepare-routes)
                                            (d/datom 1024 :route/services 1025 :prepare-routes)
                                            (d/datom 1025 :service/id :service-1 :prepare-routes)
                                            (d/datom 1025 :service/label "service-label" :prepare-routes)}
                                          {[:route/number "800"] 1024
                                           [:service/id :service-1] 1025})})))

    (testing "empty db - reverse entity-ref, isn't component"
      (let [empty-db (sut/create-conn schema)
            before @empty-db]
        (assert-report (sut/transact-sources!
                        empty-db
                        {:prepare-routes [{:vessel/imo "123"
                                           :service/_allocated-vessel [{:service/id :s567}]}]})
                       {:tx-data [[:db/add 1024 :vessel/imo "123"]
                                  [:db/add 1025 :service/id :s567]
                                  [:db/add 1025 :service/allocated-vessel 1024]]
                        :db-before before
                        :db-after (new-db before
                                          #{(d/datom 1024 :vessel/imo "123" :prepare-routes)
                                            (d/datom 1025 :service/id :s567 :prepare-routes)
                                            (d/datom 1025 :service/allocated-vessel 1024 :prepare-routes)}
                                          {[:vessel/imo "123"] 1024
                                           [:service/id :s567] 1025})})))

    (testing "empty db - reverse entity-ref, is component"
      (let [empty-db (sut/create-conn schema)
            before @empty-db]
        (assert-report (sut/transact-sources!
                        empty-db
                        {:prepare-routes [{:trip/id "foo"
                                           :service/_trips {:service/id :s567}}]})
                       {:tx-data [[:db/add 1024 :trip/id "foo"]
                                  [:db/add 1025 :service/id :s567]
                                  [:db/add 1025 :service/trips 1024]]
                        :db-before before
                        :db-after (new-db before
                                          #{(d/datom 1024 :trip/id "foo" :prepare-routes)
                                            (d/datom 1025 :service/id :s567 :prepare-routes)
                                            (d/datom 1025 :service/trips 1024 :prepare-routes)}
                                          {[:trip/id "foo"] 1024
                                           [:service/id :s567] 1025})})))

    (testing "empty db - nil val for attr throws"
      (let [empty-db (sut/create-conn schema)
            before @empty-db]
        (is (thrown? Exception
                     (sut/transact-sources!
                      empty-db
                      {:prepare-routes [{:route/number "100"
                                         :route/name nil}]})))))

    (testing "empty db - entity without identity attributes throws"
      (let [empty-db (sut/create-conn schema)
            before @empty-db]
        (is (thrown? Exception
                     (sut/transact-sources!
                      empty-db
                      {:prepare-routes [{}]})))
        (is (thrown? Exception
                     (sut/transact-sources!
                      empty-db
                      {:prepare-routes [{:route/name "Dufus"}]})))))

    (testing "conflicting values for entity attr throws"
      (let [empty-db (sut/create-conn schema)
            before @empty-db]
        (try
          (sut/transact-sources!
           empty-db
           {:prepare-routes
            [{:route/number "100" :route/name "Stavanger-Taua"}
             {:route/number "100" :route/name "Stavanger-Tau"}]})
          (is (= :should-throw :didnt))
          (catch Exception e
            (is (= "Conflicting values asserted for entity" (.getMessage e)))
            (is (= {:entity-ref [:route/number "100"]
                    :attr :route/name
                    :conflicting-values #{"Stavanger-Taua" "Stavanger-Tau"}}
                   (ex-data e))))))))

  (testing "EMPTY DB, MULTIPLE SOURCES"
    (testing "empty db two sources "
      (let [empty-db (sut/create-conn schema)
            before @empty-db]
        (assert-report (sut/transact-sources!
                        empty-db
                        {:prepare-routes [{:vessel/imo "123"
                                           :vessel/name "Fyken"}]
                         :prepare-services [{:service/id :s123
                                             :service/allocated-vessel {:vessel/imo "123"}}]})
                       {:tx-data [[:db/add 1024 :vessel/imo "123"]
                                  [:db/add 1024 :vessel/name "Fyken"]
                                  [:db/add 1025 :service/id :s123]
                                  [:db/add 1025 :service/allocated-vessel 1024]]
                        :db-before before
                        :db-after (new-db before
                                          #{(d/datom 1024 :vessel/imo "123" :prepare-routes)
                                            (d/datom 1024 :vessel/name "Fyken" :prepare-routes)
                                            (d/datom 1025 :service/id :s123 :prepare-services)
                                            (d/datom 1025 :service/allocated-vessel 1024 :prepare-services)
                                            (d/datom 1024 :vessel/imo "123" :prepare-services)}
                                          {[:vessel/imo "123"] 1024
                                           [:service/id :s123] 1025})})))

    (testing "empty db conflicting sources throws"
      (let [empty-db (sut/create-conn schema)
            before @empty-db]
        (try (sut/transact-sources!
              empty-db
              {:prepare-vessel [{:vessel/imo "123"
                                 :vessel/name "Fyken"}]
               :prepare-vessel-2 [{:vessel/imo "123"
                                   :vessel/name "Syken"}]})
             (is (= :should-throw :didnt))
             (catch Exception e
               (is (= "Conflicting values asserted between sources" (.getMessage e)))
               (is (= {:prepare-vessel [[:vessel/imo "123"] :vessel/name "Fyken"]
                       :prepare-vessel-2 [[:vessel/imo "123"] :vessel/name "Syken"]}
                      (ex-data e))))))))

  (testing "PREVIOUS DB, ONE SOURCE"
    (testing "Same entitie(s) gives no diff"
      (let [source->entity-maps {:prepare-routes [{:route/number "800" :route/name "Røvær"}]}
            before-db (sut/create-conn schema)
            _ (sut/transact-sources! before-db source->entity-maps)
            before @before-db]
        (assert-report (sut/transact-sources! before-db source->entity-maps)
                       {:tx-data []
                        :db-before before
                        :db-after before})))

    (testing "Added attr to entity gives txes and new datoms"
      (let [source->entity-maps {:prepare-routes [{:route/number "800" :route/name "Røvær"}]}
            before-db (sut/create-conn schema)
            _ (sut/transact-sources! before-db {:prepare-routes [{:route/number "800"
                                                                  :route/name "Røvær"}]})
            before @before-db]
        (assert-report (sut/transact-sources! before-db {:prepare-routes [{:route/number "800"
                                                                           :route/name "Røvær"
                                                                           :route/tags #{:tag-1 :tag-2}}]})
                       {:tx-data [[:db/add 1024 :route/tags :tag-1]
                                  [:db/add 1024 :route/tags :tag-2]]
                        :db-before before
                        :db-after (new-db before
                                          #{(d/datom 1024 :route/tags :tag-1 :prepare-routes)
                                            (d/datom 1024 :route/tags :tag-2 :prepare-routes)}
                                          {})})))

    (testing "Added new entity gives txes, new datoms and new refs"
      (let [source->entity-maps {:prepare-routes [{:route/number "800" :route/name "Røvær"}]}
            before-db (sut/create-conn schema)
            _ (sut/transact-sources! before-db {:prepare-routes [{:route/number "800"
                                                                  :route/name "Røvær"}]})
            before @before-db]
        (assert-report (sut/transact-sources! before-db {:prepare-routes [{:route/number "800"
                                                                           :route/name "Røvær"}
                                                                          {:route/number "700"
                                                                           :route/name "Døvær"}]})
                       {:tx-data [[:db/add 1025 :route/number "700"]
                                  [:db/add 1025 :route/name "Døvær"]]
                        :db-before before
                        :db-after (new-db before
                                          #{(d/datom 1025 :route/number "700" :prepare-routes)
                                            (d/datom 1025 :route/name "Døvær" :prepare-routes)}
                                          {[:route/number "700"] 1025})})))

    (testing "Added new entity and remove one gives add/remove txes, new datoms and new refs"
      (let [source->entity-maps {:prepare-routes [{:route/number "800" :route/name "Røvær"}]}
            before-db (sut/create-conn schema)
            _ (sut/transact-sources! before-db {:prepare-routes [{:route/number "800"
                                                                  :route/name "Røvær"}]})
            before @before-db]
        (assert-report (sut/transact-sources! before-db {:prepare-routes [{:route/number "700"
                                                                           :route/name "Døvær"}]})
                       {:tx-data [[:db/retract 1024 :route/number "800"]
                                  [:db/retract 1024 :route/name "Røvær"]
                                  [:db/add 1025 :route/number "700"]
                                  [:db/add 1025 :route/name "Døvær"]]
                        :db-before before
                        :db-after (assoc before
                                         :eavs #{(d/datom 1025 :route/number "700" :prepare-routes)
                                                 (d/datom 1025 :route/name "Døvær" :prepare-routes)}
                                         :refs {[:route/number "700"] 1025
                                                [:route/number "800"] 1024})})))))

