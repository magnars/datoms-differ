(ns datoms-differ.api-test
  (:require [datoms-differ.api :as sut]
            [datoms-differ.datom :as d]
            [datoms-differ.impl.core-helpers :as ch]
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

(def attrs (ch/find-attrs schema))

(deftest create-conn
  (is (= @(sut/create-conn schema)
         {:schema schema
          :refs {}
          :eavs #{}})))

(defn assert-report [report {:keys [tx-data refs eavs refs-added eavs-added]}]
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
  @(sut/create-conn schema))

(deftest with
  (testing "source not keyword throws"
    (is (thrown-with-msg? Exception
                          #"Source must be a keyword"
                          (sut/with (db) "prepare" []))))

  (testing "EMPTY DB, ONE SOURCE"

    (testing "empty db no entities returns same"
      (let [empty-db (db)]
        (is (= (sut/with empty-db :prepare [])
               {:tx-data []
                :db-before empty-db
                :db-after empty-db}))))

    (testing "empty db one entity"
      (assert-report
       (sut/with (db) :prepare-routes [{:route/number "800"
                                        :route/name "Røvær"}])
       {:tx-data [[:db/add 1024 :route/name "Røvær"]
                  [:db/add 1024 :route/number "800"]]
        :eavs #{(d/datom 1024 :route/number "800" :prepare-routes)
                (d/datom 1024 :route/name "Røvær" :prepare-routes)}
        :refs {[:route/number "800"] 1024}}))

    (testing "empty db one entity and cardinality many"
      (assert-report
       (sut/with (db) :prepare-routes [{:route/number "800"
                                        :route/tags #{:tag-1 :tag-2}}])

       {:tx-data [[:db/add 1024 :route/number "800"]
                  [:db/add 1024 :route/tags :tag-1]
                  [:db/add 1024 :route/tags :tag-2]]

        :eavs #{(d/datom 1024 :route/number "800" :prepare-routes)
                (d/datom 1024 :route/tags :tag-1 :prepare-routes)
                (d/datom 1024 :route/tags :tag-2 :prepare-routes)}
        :refs {[:route/number "800"] 1024}}))

    (testing "empty db one entity with nested entities"
      (assert-report
       (sut/with (db)
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
      (assert-report
       (sut/with
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
      (assert-report
       (sut/with
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
      (assert-report
       (sut/with
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
      (is (thrown-with-msg? Exception #"Attributes cannot be nil"
                            (sut/with (db) :prepare-routes [{:route/number "100"
                                                             :route/name nil}]))))

    (testing "Running out of eids for db-id-partition throws"
      (let [db (sut/create-conn (assoc schema ::sut/db-id-partition {:from 1 :to 2}))]
        (is
         (thrown-with-msg? Exception #"Generated internal eid falls outside internal db-id-partition,"
                           (sut/with @db :prep [{:vessel/imo "123"}
                                                {:vessel/imo "234"}
                                                {:vessel/imo "345"}])))))

    (testing "empty db - entity without identity attributes throws"
      (is (thrown? Exception
                   (sut/with (db) :prepare-routes [{}])))
      (is (thrown? Exception
                   (sut/with (db) :prepare-routes [{:route/name "Dufus"}]))))

    (testing "conflicting values for entity attr throws"
      (try
        (sut/with (db) :prepare-routes [{:route/number "100" :route/name "Stavanger-Taua"}
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
      (let [{:keys [db-after]} (sut/with (db) :prepare-routes [{:route/number "800" :route/name "Røvær"}])]
        (assert-report
         (sut/with db-after :prepare-routes [{:route/number "800" :route/name "Røvær"}])
         {:tx-data []
          :refs (:refs db-after)
          :eavs (:eavs db-after)})))

    (testing "Added attr to entity gives txes and new datoms"
      (let [{:keys [db-after]} (sut/with (db) :prepare-routes [{:route/number "800"
                                                                :route/name "Røvær"}])]
        (assert-report
         (sut/with db-after :prepare-routes [{:route/number "800"
                                              :route/name "Røvær"
                                              :route/tags #{:tag-1 :tag-2}}])

         {:tx-data [[:db/add 1024 :route/tags :tag-1]
                    [:db/add 1024 :route/tags :tag-2]]
          :eavs-added #{(d/datom 1024 :route/tags :tag-1 :prepare-routes)
                        (d/datom 1024 :route/tags :tag-2 :prepare-routes)}
          :refs-added {}})))

    (testing "Added new entity gives txes, new datoms and new refs"
      (let [{:keys [db-after]} (sut/with (db) :prepare-routes [{:route/number "800"
                                                                :route/name "Røvær"}])]
        (assert-report
         (sut/with db-after :prepare-routes [{:route/number "800"
                                              :route/name "Røvær"}
                                             {:route/number "700"
                                              :route/name "Døvær"}])

         {:tx-data [[:db/add 1025 :route/name "Døvær"]
                    [:db/add 1025 :route/number "700"]]
          :eavs-added #{(d/datom 1025 :route/number "700" :prepare-routes)
                        (d/datom 1025 :route/name "Døvær" :prepare-routes)}
          :refs-added {[:route/number "700"] 1025}})))

    (testing "Added new entity and remove one gives add/remove txes, new datoms and new refs"
      (let [{:keys [db-after]} (sut/with (db) :prepare-routes [{:route/number "800"
                                                                :route/name "Røvær"}])]
        (assert-report
         (sut/with db-after :prepare-routes [{:route/number "700"
                                              :route/name "Døvær"}])

         {:tx-data [[:db/retract 1024 :route/name "Røvær"]
                    [:db/retract 1024 :route/number "800"]
                    [:db/add 1025 :route/name "Døvær"]
                    [:db/add 1025 :route/number "700"]]
          :eavs #{(d/datom 1025 :route/number "700" :prepare-routes)
                  (d/datom 1025 :route/name "Døvær" :prepare-routes)}
          :refs {[:route/number "700"] 1025}})))

    (testing "Remove one entity of many uses optimized path for ref pruning"
      (let [routes (for [i (range 1 8)] {:route/number (str i)})
            {:keys [db-after]} (sut/with (db) :prepare-routes (concat
                                                               [{:route/number "800"}]
                                                               routes))]
        (assert-report
         (sut/with db-after :prepare-routes routes)

         {:tx-data [[:db/retract 1024 :route/number "800"]]
          :refs (-> db-after :refs (dissoc [:route/number "800"]))})))

    (testing "Modify value to conflict for optimized check throws"
      (let [entities (concat [{:vessel/imo "123" :vessel/name "Fyken"}]
                             (for [i (range 100)]
                               {:vessel/imo (str i) :vessel/name (str i)}))]
        (try (-> (sut/with (db) :source entities)
                 :db-after
                 (sut/with :source (concat entities [{:vessel/imo "123" :vessel/name "Syken"}])))
             (is (= :should-throw :didnt))
               (catch Exception e
                 (is (= "Conflicting values asserted for entity" (.getMessage e)))
                 (is (= {:attr :vessel/name
                         :entity-ref [:vessel/imo "123"]
                         :conflict {:source #{"Fyken" "Syken"}}}
                        (ex-data e)))))))))

(deftest with-sources
  (testing "source not keyword throws"
    (is (thrown-with-msg? Exception
                          #"Source must be a keyword"
                          (sut/with-sources (db) {:source []
                                                  "source-2" []}))))

  (testing "EMPTY DB, MULTIPLE SOURCES"
    (testing "empty db two sources "
      (assert-report
       (sut/with-sources
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
               [:service/id :s123] 1025}}))

    (testing "empty db conflicting sources throws"
      (try (sut/with-sources
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
      (try (sut/with-sources
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
                   (sut/with :source-1 tx-source-1)
                   :db-after
                   (sut/with :source-2 tx-source-2)
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
      (let [{:keys [db-after]} (sut/with-sources (db) {:prepare-vessels [{:vessel/imo "123"
                                                                          :vessel/name "Fyken"}
                                                                         {:vessel/imo "234"
                                                                          :vessel/name "Syken"}]
                                                       :prepare-services [{:service/id :s123
                                                                           :service/allocated-vessel {:vessel/imo "123"}
                                                                           :service/label "fast"}
                                                                          {:service/id :s234
                                                                           :service/allocated-vessel {:vessel/imo "234"}
                                                                           :service/label "slow"}]})]
        (assert-report
         (sut/with-sources db-after {:prepare-vessels [{:vessel/imo "234"
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

(deftest transact!
  (testing "transact! works as with only wrapped with an atom"
    (assert-report
     (sut/transact! (atom (db)) :dummy [{:vessel/imo "100"}])

     {:eavs #{(d/datom 1024 :vessel/imo "100" :dummy)}
      :refs {[:vessel/imo "100"] 1024}})))

(deftest transact-sources!
  (testing "transact-sources! works as with-sources only wrapped with an atom"
    (assert-report
     (sut/transact-sources! (atom (db)) {:dummy [{:vessel/imo "100"}]})

     {:eavs #{(d/datom 1024 :vessel/imo "100" :dummy)}
      :refs {[:vessel/imo "100"] 1024}})))

(deftest explode
  (testing "explode happy days"
    (is (= (sut/explode schema [{:vessel/imo "123" :vessel/name "Fyken"}])
           {:refs {[:vessel/imo "123"] 1024}
            :datoms [[1024 :vessel/imo "123"]
                     [1024 :vessel/name "Fyken"]]})))

  (testing "explode throws on conflicting values for attribute"
    (try (sut/explode schema [{:vessel/imo "123" :vessel/name "Fyken"}
                              {:vessel/imo "123" :vessel/name "Syken"}])
         (is (= :should-throw :didnt))
         (catch Exception e
           (is (= "Conflicting values asserted for entity" (.getMessage e)))
           (is (= {:attr :vessel/name
                   :entity-ref [:vessel/imo "123"]
                   :conflicting-values #{"Fyken" "Syken"}}
                  (ex-data e))))))

  (testing "explode throws on conflicting refs and contains lookup-refs in ex"
    (try (sut/explode schema [{:service/id :s567 :service/allocated-vessel {:vessel/imo "123"}}
                              {:service/id :s567 :service/allocated-vessel {:vessel/imo "456"}}])
         (is (= :should-throw :didnt))
         (catch Exception e
           (is (= "Conflicting values asserted for entity" (.getMessage e)))
           (is (= {:attr :service/allocated-vessel
                   :entity-ref [:service/id :s567]
                   :conflicting-values #{[:vessel/imo "123"] [:vessel/imo "456"]}}
                  (ex-data e)))))))

(deftest get-datoms []
  (testing "Get all datoms for db with one source"
    (is (= (-> (sut/with (db) :dummy [{:vessel/imo "123" :vessel/name "Fyken"}
                                      {:vessel/imo "234" :vessel/name "Syken"}])
               :db-after
               sut/get-datoms)
           [[1024 :vessel/imo "123"]
            [1024 :vessel/name "Fyken"]
            [1025 :vessel/imo "234"]
            [1025 :vessel/name "Syken"]])))

  (testing "Get all datoms for db with multiple sources"
    (is (= (-> (sut/with-sources (db) {:source-1 [{:vessel/imo "123" :vessel/name "Fyken"}
                                                  {:vessel/imo "234" :vessel/name "Syken"}]
                                       :source-2 [{:service/id :s1 :service/allocated-vessel {:vessel/imo "123"}}
                                                  {:service/id :s2 :service/allocated-vessel {:vessel/imo "234"}}]})
               :db-after
               sut/get-datoms)
           [[1024 :vessel/imo "123"]
            [1024 :vessel/name "Fyken"]
            [1025 :vessel/imo "234"]
            [1025 :vessel/name "Syken"]
            [1026 :service/allocated-vessel 1024]
            [1026 :service/id :s1]
            [1027 :service/allocated-vessel 1025]
            [1027 :service/id :s2]]))))

(deftest export-db
  (testing "Export db regression test"
    (let [{:keys [db-after]} (sut/with (db) :dummy [{:vessel/imo "123" :vessel/name "Fyken"}])]
      (is (= (sut/export-db db-after)
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
                  "[[1024 :vessel/imo \"123\" 536870912]"
                  " [1024 :vessel/name \"Fyken\" 536870912]]"
                  "}"))))))

(deftest prune-diffs
  (testing "Prune diffs regression test"
    (is (= (sut/prune-diffs schema
                            [[:db/retract 2025 :service/allocated-vessel 2026]
                             [:db/add 2025 :service/allocated-vessel 2027]])
           [[:db/add 2025 :service/allocated-vessel 2027]]))))
