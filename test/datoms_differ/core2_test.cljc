(ns ^:focus datoms-differ.core2-test
  (:require [datoms-differ.core2 :as sut]
            [datoms-differ.datom :as d]
            [clojure.test :refer [deftest is testing]]
            [me.tonsky.persistent-sorted-set :as set]
            [clojure.string :as str]))

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
          :refs {}
          :eavs #{}})))

(defn assert-report-2 [report {:keys [tx-data refs eavs refs-added eavs-added]}]
  (when tx-data (is (= (:tx-data report) tx-data) "Difference in tx-data"))
  (when refs (is (= (-> report :db-after :refs) refs) "Difference in refs"))
  (when eavs (is (= (-> report :db-after :eavs) eavs) "Difference in eavs"))
  (when refs-added
    (is (= (-> report :db-after :refs)
           (merge (-> report :db-after :refs) refs-added))
        "Difference in refs (added)"))
  (when eavs-added
    (is (= (-> report :db-after :eavs)
           (clojure.set/union (-> report :db-after :eavs) eavs-added))
        "Difference in eavs (added)")))

(defn db []
  (sut/create-conn schema))

(deftest transact!
  (testing "EMPTY DB, ONE SOURCE"

    (testing "empty db no entities returns same"
      (let [empty-db (sut/create-conn schema)]
        (is (= (sut/transact! empty-db :prepare [])
               {:tx-data []
                :db-before @empty-db
                :db-after @empty-db}))))

    (testing "empty db one entity"
      (assert-report-2
       (sut/transact! (db) :prepare-routes [{:route/number "800"
                                             :route/name "Røvær"}])
       {:tx-data [[:db/add 1024 :route/name "Røvær"]
                  [:db/add 1024 :route/number "800"]]
        :eavs #{(d/datom 1024 :route/number "800" :prepare-routes)
                (d/datom 1024 :route/name "Røvær" :prepare-routes)}
        :refs {[:route/number "800"] 1024}}))

    (testing "empty db one entity and cardinality many"
      (assert-report-2
       (sut/transact! (db) :prepare-routes [{:route/number "800"
                                             :route/tags #{:tag-1 :tag-2}}])

       {:tx-data [[:db/add 1024 :route/number "800"]
                  [:db/add 1024 :route/tags :tag-1]
                  [:db/add 1024 :route/tags :tag-2]]

        :eavs #{(d/datom 1024 :route/number "800" :prepare-routes)
                (d/datom 1024 :route/tags :tag-1 :prepare-routes)
                (d/datom 1024 :route/tags :tag-2 :prepare-routes)}
        :refs {[:route/number "800"] 1024}}))

    (testing "empty db one entity with nested entities"
      (assert-report-2
       (sut/transact! (db)
                      :prepare-routes [{:route/number "800"
                                        :route/name "Røvær"
                                        :route/services [{:service/id :service-1
                                                          :service/label "service-label"}]}])

       {:tx-data [[:db/add 1024 :route/name "Røvær"]
                  [:db/add 1024 :route/number "800"]
                  [:db/add 1024 :route/services 1025]
                  [:db/add 1025 :service/id :service-1]
                  [:db/add 1025 :service/label "service-label"]]
        :eavs #{(d/datom 1024 :route/number "800" :prepare-routes)
                (d/datom 1024 :route/name "Røvær" :prepare-routes)
                (d/datom 1024 :route/services 1025 :prepare-routes)
                (d/datom 1025 :service/id :service-1 :prepare-routes)
                (d/datom 1025 :service/label "service-label" :prepare-routes)}
        :refs {[:route/number "800"] 1024
               [:service/id :service-1] 1025}}))

    (testing "empty db - reverse entity-ref, isn't component"
      (assert-report-2
       (sut/transact!
        (db)
        :prepare-routes [{:vessel/imo "123"
                          :service/_allocated-vessel [{:service/id :s567}]}])

       {:tx-data [[:db/add 1024 :vessel/imo "123"]
                  [:db/add 1025 :service/allocated-vessel 1024]
                  [:db/add 1025 :service/id :s567]]
        :eavs #{(d/datom 1024 :vessel/imo "123" :prepare-routes)
                (d/datom 1025 :service/id :s567 :prepare-routes)
                (d/datom 1025 :service/allocated-vessel 1024 :prepare-routes)}
        :refs {[:vessel/imo "123"] 1024
               [:service/id :s567] 1025}}))

    (testing "empty db - reverse entity-ref, is component"
      (assert-report-2
       (sut/transact!
        (db)
        :prepare-routes [{:trip/id "foo"
                          :service/_trips {:service/id :s567}}])

       {:tx-data [[:db/add 1024 :trip/id "foo"]
                  [:db/add 1025 :service/id :s567]
                  [:db/add 1025 :service/trips 1024]]
        :eavs #{(d/datom 1024 :trip/id "foo" :prepare-routes)
                (d/datom 1025 :service/id :s567 :prepare-routes)
                (d/datom 1025 :service/trips 1024 :prepare-routes)}
        :refs {[:trip/id "foo"] 1024
               [:service/id :s567] 1025}}))

    (testing "empty-db - reverse refs multiple levels"
      (assert-report-2
       (sut/transact!
        (db)
        :prep [{:vessel/imo "123"
                :service/_allocated-vessel [{:service/id :s567
                                             :route/_services #{{:route/number "100"}}}]}
               {:trip/id "foo"
                :service/_trips {:service/id :s789
                                 :route/_services #{{:route/number "100"}}
                                 :service/trips #{{:trip/id "bar"}}}}])
       {:tx-data [[:db/add 1024 :vessel/imo "123"]
                  [:db/add 1025 :trip/id "foo"]
                  [:db/add 1026 :service/allocated-vessel 1024]
                  [:db/add 1026 :service/id :s567]
                  [:db/add 1027 :route/number "100"]
                  [:db/add 1027 :route/services 1026]
                  [:db/add 1027 :route/services 1028]
                  [:db/add 1028 :service/id :s789]
                  [:db/add 1028 :service/trips 1025]
                  [:db/add 1028 :service/trips 1029]
                  [:db/add 1029 :trip/id "bar"]]
        :eavs #{(d/datom 1024 :vessel/imo "123" :prep)
                (d/datom 1025 :trip/id "foo" :prep)
                (d/datom 1029 :trip/id "bar" :prep)
                (d/datom 1026 :service/allocated-vessel 1024 :prep)
                (d/datom 1026 :service/id :s567 :prep)
                (d/datom 1027 :route/number "100" :prep)
                (d/datom 1027 :route/services 1026 :prep)
                (d/datom 1027 :route/services 1028 :prep)
                (d/datom 1028 :service/id :s789 :prep)
                (d/datom 1028 :service/trips 1025 :prep)
                (d/datom 1028 :service/trips 1029 :prep)}
        :refs {[:route/number "100"] 1027
               [:service/id :s567] 1026
               [:service/id :s789] 1028
               [:trip/id "foo"] 1025
               [:trip/id "bar"] 1029
               [:vessel/imo "123"] 1024}}))

    (testing "empty db - nil val for attr throws"
      (is (thrown? Exception
                   (sut/transact! (db) :prepare-routes [{:route/number "100"
                                                         :route/name nil}]))))

    (testing "Running out of eids for db-id-partition throws"
      (let [db (sut/create-conn (assoc schema ::sut/db-id-partition {:from 1 :to 2}))]
        (try
          (sut/transact! db :prep [{:vessel/imo "123"}
                                   {:vessel/imo "234"}
                                   {:vessel/imo "345"}])
          (is (= :should-throw :didnt))
          (catch Exception e
            (is (str/starts-with? (.getMessage e) "Generated internal eid falls outside internal db-id-partition,"))))))

    (testing "empty db - entity without identity attributes throws"
      (is (thrown? Exception
                   (sut/transact! (db) :prepare-routes [{}])))
      (is (thrown? Exception
                   (sut/transact! (db) :prepare-routes [{:route/name "Dufus"}]))))

    (testing "conflicting values for entity attr throws"
      (try
        (sut/transact! (db) :prepare-routes [{:route/number "100" :route/name "Stavanger-Taua"}
                                            {:route/number "100" :route/name "Stavanger-Tau"}])
        (is (= :should-throw :didnt))
        (catch Exception e
          (is (= "Conflicting values asserted for entity" (.getMessage e)))
          (is (= {:entity-ref [:route/number "100"]
                  :attr :route/name
                  :conflict {:prepare-routes #{"Stavanger-Taua" "Stavanger-Tau"}}}
                 (ex-data e)))))))

  (testing "PREVIOUS DB, ONE SOURCE"
    (testing "Same entitie(s) gives no diff"
      (let [before-db (db)
            _ (sut/transact! before-db :prepare-routes [{:route/number "800" :route/name "Røvær"}])
            before @before-db]
        (assert-report-2
         (sut/transact! before-db :prepare-routes [{:route/number "800" :route/name "Røvær"}])
         {:tx-data []
          :refs (:refs before)
          :eavs (:eavs before)})))

    (testing "Added attr to entity gives txes and new datoms"
      (let [before-db (db)
            _ (sut/transact! before-db :prepare-routes [{:route/number "800"
                                                         :route/name "Røvær"}])
            before @before-db]
        (assert-report-2
         (sut/transact! before-db :prepare-routes [{:route/number "800"
                                                    :route/name "Røvær"
                                                    :route/tags #{:tag-1 :tag-2}}])

         {:tx-data [[:db/add 1024 :route/tags :tag-1]
                    [:db/add 1024 :route/tags :tag-2]]
          :eavs-added #{(d/datom 1024 :route/tags :tag-1 :prepare-routes)
                        (d/datom 1024 :route/tags :tag-2 :prepare-routes)}
          :refs-added {}})))

    (testing "Added new entity gives txes, new datoms and new refs"
      (let [before-db (db)
            _ (sut/transact! before-db :prepare-routes [{:route/number "800"
                                                         :route/name "Røvær"}])
            before @before-db]
        (assert-report-2
         (sut/transact! before-db :prepare-routes [{:route/number "800"
                                                    :route/name "Røvær"}
                                                   {:route/number "700"
                                                    :route/name "Døvær"}])

         {:tx-data [[:db/add 1025 :route/name "Døvær"]
                    [:db/add 1025 :route/number "700"]]
          :eavs-added #{(d/datom 1025 :route/number "700" :prepare-routes)
                        (d/datom 1025 :route/name "Døvær" :prepare-routes)}
          :refs-added {[:route/number "700"] 1025}})))

    (testing "Added new entity and remove one gives add/remove txes, new datoms and new refs"
      (let [before-db (db)
            _ (sut/transact! before-db :prepare-routes [{:route/number "800"
                                                         :route/name "Røvær"}])
            before @before-db]
        (assert-report-2
         (sut/transact! before-db :prepare-routes [{:route/number "700"
                                                    :route/name "Døvær"}])

         {:tx-data [[:db/retract 1024 :route/name "Røvær"]
                    [:db/retract 1024 :route/number "800"]
                    [:db/add 1025 :route/name "Døvær"]
                    [:db/add 1025 :route/number "700"]]
          :eavs-added #{(d/datom 1025 :route/number "700" :prepare-routes)
                        (d/datom 1025 :route/name "Døvær" :prepare-routes)}
          :refs-added {[:route/number "700"] 1025
                       [:route/number "800"] 1024}})))))

(deftest transact-sources!
  (testing "EMPTY DB, MULTIPLE SOURCES"
    (testing "empty db two sources "
      (let [empty-db (sut/create-conn schema)
            before @empty-db]
        (assert-report-2
         (sut/transact-sources!
          (db)
          {:prepare-routes [{:vessel/imo "123"
                             :vessel/name "Fyken"}]
           :prepare-services [{:service/id :s123
                               :service/allocated-vessel {:vessel/imo "123"}}]})

         {:tx-data [[:db/add 1024 :vessel/imo "123"]
                    [:db/add 1024 :vessel/name "Fyken"]
                    [:db/add 1025 :service/allocated-vessel 1024]
                    [:db/add 1025 :service/id :s123]]
          :eavs #{(d/datom 1024 :vessel/imo "123" :prepare-routes)
                  (d/datom 1024 :vessel/name "Fyken" :prepare-routes)
                  (d/datom 1025 :service/id :s123 :prepare-services)
                  (d/datom 1025 :service/allocated-vessel 1024 :prepare-services)
                  (d/datom 1024 :vessel/imo "123" :prepare-services)}
          :refs {[:vessel/imo "123"] 1024
                 [:service/id :s123] 1025}})))

    (testing "empty db conflicting sources throws"
      (try (sut/transact-sources!
            (db)
            {:prepare-vessel [{:vessel/imo "123" :vessel/name "Fyken"}]
             :prepare-vessel-2 [{:vessel/imo "123" :vessel/name "Syken"}]})
           (is (= :should-throw :didnt))
           (catch Exception e
             (is (= "Conflicting values asserted for entity" (.getMessage e)))
             (is (= {:attr :vessel/name
                     :entity-ref [:vessel/imo "123"]
                     :conflict {:prepare-vessel #{"Fyken"}
                                :prepare-vessel-2 #{"Syken"}}}
                    (ex-data e))))))

    (testing "empty db conflicting sources throws"
      (try (sut/transact-sources!
            (db)
            {:prepare-vessel [{:vessel/imo "123" :vessel/name "Fyken"}]
             :prepare-vessel-2 [{:vessel/imo "123" :vessel/name "Syken"}]})
           (is (= :should-throw :didnt))
           (catch Exception e
             (is (= "Conflicting values asserted for entity" (.getMessage e)))
             (is (= {:attr :vessel/name
                     :entity-ref [:vessel/imo "123"]
                     :conflict {:prepare-vessel #{"Fyken"}
                                :prepare-vessel-2 #{"Syken"}}}
                    (ex-data e))))))

    (testing "empty db conflicting values throws and includes entity ref for refs"
      (try (sut/transact-sources!
            (db)
            {:source-1 [{:service/id :s567 :service/allocated-vessel {:vessel/imo "123"}}]
             :source-2 [{:service/id :s567 :service/allocated-vessel {:vessel/imo "456"}}]})
           (is (= :should-throw :didnt))
           (catch Exception e
             (is (= "Conflicting values asserted for entity" (.getMessage e)))
             (is (= {:attr :service/allocated-vessel
                     :entity-ref [:service/id :s567]
                     :conflict {:source-1 #{[:vessel/imo "123"]}
                                :source-2 #{[:vessel/imo "456"]}}}
                    (ex-data e))))))

    (testing "does not throw when new entities are asserted for a cardinality many attr"
      (let [db-at-first (db)
            tx-source-1 [{:service/id :s567 :service/trips #{{:trip/id "bar"}}}]
            tx-source-2 [{:service/id :s567 :service/trips #{{:trip/id "foo"}}}]]
        (is (= (-> db-at-first
                   (sut/transact! :source-1 tx-source-1)
                   :db-after
                   (atom)
                   (sut/transact! :source-2 tx-source-2)
                   :db-after
                   :eavs)
               #{(d/datom 1024 :service/id :s567 :source-1)
                 (d/datom 1024 :service/id :s567 :source-2)
                 (d/datom 1024 :service/trips 1025 :source-1)
                 (d/datom 1024 :service/trips 1026 :source-2)
                 (d/datom 1025 :trip/id "bar" :source-1)
                 (d/datom 1026 :trip/id "foo" :source-2)})))))

  (testing "PREVIOUS DB, MULTIPLE SOURCES"
    (testing "Add and remove across sources"
      (let [before-db (sut/create-conn schema)
            _ (sut/transact-sources! before-db {:prepare-vessels [{:vessel/imo "123"
                                                                   :vessel/name "Fyken"}
                                                                  {:vessel/imo "234"
                                                                   :vessel/name "Syken"}]
                                                :prepare-services [{:service/id :s123
                                                                    :service/allocated-vessel {:vessel/imo "123"}
                                                                    :service/label "fast"}
                                                                   {:service/id :s234
                                                                    :service/allocated-vessel {:vessel/imo "234"}
                                                                    :service/label "slow"}]})
            before @before-db]
        (assert-report-2
         (sut/transact-sources! before-db {:prepare-vessels [{:vessel/imo "234"
                                                              :vessel/name "Tyken"}
                                                             {:vessel/imo "345"
                                                              :vessel/name "Titanic"}]
                                           :prepare-services [{:service/id :s123
                                                               :service/allocated-vessel {:vessel/imo "123"}
                                                               :service/label "fast"}
                                                              {:service/id :s234
                                                               :service/allocated-vessel {:vessel/imo "345"}
                                                               :service/label "slow"}]})

         {:tx-data [[:db/retract 1024 :vessel/name "Fyken"]
                    [:db/retract 1025 :vessel/name "Syken"]
                    [:db/retract 1027 :service/allocated-vessel 1025]
                    [:db/add 1025 :vessel/name "Tyken"]
                    [:db/add 1027 :service/allocated-vessel 1028]
                    [:db/add 1028 :vessel/imo "345"]
                    [:db/add 1028 :vessel/name "Titanic"]]
          :eavs #{(d/datom 1024 :vessel/imo "123" :prepare-services)
                  (d/datom 1025 :vessel/imo "234" :prepare-vessels)
                  (d/datom 1025 :vessel/name "Tyken" :prepare-vessels)
                  (d/datom 1026 :service/allocated-vessel 1024 :prepare-services)
                  (d/datom 1026 :service/id :s123 :prepare-services)
                  (d/datom 1026 :service/label "fast" :prepare-services)
                  (d/datom 1027 :service/allocated-vessel 1028 :prepare-services)
                  (d/datom 1027 :service/id :s234 :prepare-services)
                  (d/datom 1027 :service/label "slow" :prepare-services)
                  (d/datom 1028 :vessel/imo "345" :prepare-services)
                  (d/datom 1028 :vessel/imo "345" :prepare-vessels)
                  (d/datom 1028 :vessel/name "Titanic" :prepare-vessels)}
          :refs {[:vessel/imo "123"] 1024
                 [:vessel/imo "234"] 1025
                 [:service/id :s123] 1026
                 [:service/id :s234] 1027
                 [:vessel/imo "345"] 1028}})))))

