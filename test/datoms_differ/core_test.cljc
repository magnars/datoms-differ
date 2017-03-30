(ns datoms-differ.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [datoms-differ.core :as sut]))

(def schema
  {:route/name {}
   :route/number {:db/unique :db.unique/identity}
   :route/services {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
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
          :many? #{:route/services :service/trips}
          :component? #{:service/trips}})))

(def attrs (sut/find-attrs schema))

(deftest gets-entity-refs
  (is (= (sut/get-entity-ref attrs {:route/number "100" :route/name "Stavanger-Tau"})
         [:route/number "100"]))

  (is (thrown? Exception ;; multiple identity attributes
               (sut/get-entity-ref attrs {:route/number "100" :service/id 200})))

  (is (thrown? Exception ;; no identity attributes
               (sut/get-entity-ref attrs {:route/name "Stavanger-Tau"}))))

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

(deftest creates-new-eids-for-unknown-entity-refs
  (is (= (sut/create-refs-lookup {[:route/number "100"] 1024
                                  [:vessel/imo "123"] 1025}
                                 [[:route/number "100"]
                                  [:vessel/imo "123"]
                                  [:service/id :s567]])
         {[:route/number "100"] 1024
          [:vessel/imo "123"] 1025
          [:service/id :s567] 1026})))

(deftest flattens-entity-map
  (is (= (sut/flatten-entity-map attrs
                                 {[:route/number "100"] 1024
                                  [:vessel/imo "123"] 1025
                                  [:service/id :s567] 1026}
                                 {:route/number "100"
                                  :route/name "Stavanger-Tau"})
         [[1024 :route/number "100"]
          [1024 :route/name "Stavanger-Tau"]]))

  ;; reverse entity ref, isn't component
  (is (= (sut/flatten-entity-map attrs
                                 {[:service/id :s567] 1026
                                  [:vessel/imo "123"] 1024}
                                 {:vessel/imo "123"
                                  :service/_allocated-vessel [{:service/id :s567}]})
         [[1024 :vessel/imo "123"]
          [1026 :service/allocated-vessel 1024]]))

  ;; reverse entity ref, is component
  (is (= (sut/flatten-entity-map attrs
                                 {[:service/id :s567] 1026
                                  [:trip/id "foo"] 1025}
                                 {:trip/id "foo"
                                  :service/_trips {:service/id :s567}})
         [[1025 :trip/id "foo"]
          [1026 :service/trips 1025]]))

  (is (= (sut/flatten-entity-map attrs
                                 {[:route/number "100"] 1024
                                  [:vessel/imo "123"] 1025
                                  [:service/id :s567] 1026}
                                 {:route/number "100"
                                  :route/services [{:service/id :s567
                                                    :service/allocated-vessel {:vessel/imo "123"}}]})
         [[1024 :route/number "100"]
          [1024 :route/services 1026]]))

  (is (thrown? Exception ;; no nil vals
               (sut/flatten-entity-map attrs
                                       {[:route/number "100"] 1024
                                        [:vessel/imo "123"] 1025
                                        [:service/id :s567] 1026}
                                       {:route/number "100"
                                        :route/name nil}))))

(deftest explodes
  (testing "no existing refs, simple case"
    (is (= (sut/explode {:schema schema :refs {}}
                        [{:route/number "100"}])
           {:refs {[:route/number "100"] 1024}
            :datoms #{[1024 :route/number "100"]}})))

  (testing "no existing refs, interesting case"
    (is (= (sut/explode {:schema schema :refs {}}
                        [{:route/number "100"
                          :route/services [{:service/id :s567
                                            :service/allocated-vessel {:vessel/imo "123"}}
                                           {:service/id :s789}]}])
           {:refs {[:route/number "100"] 1024
                   [:service/id :s567] 1025
                   [:service/id :s789] 1026
                   [:vessel/imo "123"] 1027}
            :datoms #{[1024 :route/number "100"]
                      [1024 :route/services 1025]
                      [1024 :route/services 1026]
                      [1025 :service/id :s567]
                      [1025 :service/allocated-vessel 1027]
                      [1026 :service/id :s789]
                      [1027 :vessel/imo "123"]}})))

  (testing "reverse refs"
    (is (= {:refs {[:route/number "100"] 1027
                   [:service/id :s567] 1026
                   [:service/id :s789] 1025
                   [:vessel/imo "123"] 1024}
            :datoms #{[1027 :route/number "100"]
                      [1027 :route/services 1026]
                      [1027 :route/services 1025]
                      [1026 :service/id :s567]
                      [1026 :service/allocated-vessel 1024]
                      [1025 :service/id :s789]
                      [1024 :vessel/imo "123"]}}
           (sut/explode {:schema schema :refs {}}
                        [{:vessel/imo "123"
                          :service/_allocated-vessel [{:service/id :s567
                                                       :route/_services #{{:route/number "100"}}}]}
                         {:service/id :s789
                          :route/_services [{:route/number "100"}]}]))))

  (testing "existing refs"
    (is (= (sut/explode {:schema schema :refs {[:route/number "100"] 2024
                                               [:service/id :s567] 2025}}
                        [{:route/number "100"
                          :route/services [{:service/id :s567
                                            :service/allocated-vessel {:vessel/imo "123"}}]}])
           {:refs {[:route/number "100"] 2024
                   [:service/id :s567] 2025
                   [:vessel/imo "123"] 2026}
            :datoms #{[2024 :route/number "100"]
                      [2024 :route/services 2025]
                      [2025 :service/id :s567]
                      [2025 :service/allocated-vessel 2026]
                      [2026 :vessel/imo "123"]}})))

  (testing "conflicting values for entity attr"
    (is (thrown? Exception ;; different values for same entity attribute
                 (sut/explode {:schema schema :refs {}}
                              [{:route/number "100" :route/name "Stavanger-Taua"}
                               {:route/number "100" :route/name "Stavanger-Tau"}])))))

(deftest diffs
  (is (= (sut/diff #{}
                   #{[2025 :service/id :s567]
                     [2025 :service/allocated-vessel 2026]
                     [2026 :vessel/imo "123"]})
         [[:db/add 2025 :service/id :s567]
          [:db/add 2025 :service/allocated-vessel 2026]
          [:db/add 2026 :vessel/imo "123"]]))

  (is (= (sut/diff #{[2025 :service/id :s567]
                     [2025 :service/allocated-vessel 2026]
                     [2026 :vessel/imo "123"]}
                   #{})
         [[:db/retract 2025 :service/id :s567]
          [:db/retract 2025 :service/allocated-vessel 2026]
          [:db/retract 2026 :vessel/imo "123"]]))

  (is (= (sut/diff #{[2024 :service/id :s345] [2025 :service/id :s567] [2025 :service/allocated-vessel 2026] [2026 :vessel/imo "123"]}
                   #{[2024 :service/id :s345] [2025 :service/id :s567] [2024 :service/allocated-vessel 2026] [2026 :vessel/imo "123"]})
         [[:db/retract 2025 :service/allocated-vessel 2026]
          [:db/add 2024 :service/allocated-vessel 2026]])))

(deftest with-keeps-track-of-different-sources
  (let [db-at-first (sut/empty-db schema)
        tx-sporadic-1 [{:route/number "100"
                        :route/services [{:service/id :s567
                                          :service/allocated-vessel {:vessel/imo "123"}}]}]
        refs {[:route/number "100"] 1024
              [:service/id :s567] 1025
              [:vessel/imo "123"] 1026}

        sporadic-1-datoms #{[1024 :route/number "100"]
                            [1024 :route/services 1025]
                            [1025 :service/id :s567]
                            [1025 :service/allocated-vessel 1026]
                            [1026 :vessel/imo "123"]}

        tx-data-sporadic-1 #{[:db/add 1024 :route/number "100"]
                             [:db/add 1024 :route/services 1025]
                             [:db/add 1025 :service/id :s567]
                             [:db/add 1025 :service/allocated-vessel 1026]
                             [:db/add 1026 :vessel/imo "123"]}

        db-after-sporadic-1 {:schema schema
                             :refs refs
                             :source-datoms {:sporadic sporadic-1-datoms}}

        tx-frequent-1 [{:vessel/imo "123" :vessel/name "Jekyll"}]

        tx-data-frequent-1 #{[:db/add 1026 :vessel/name "Jekyll"]}

        db-after-frequent-1 {:schema schema
                             :refs refs
                             :source-datoms {:sporadic sporadic-1-datoms
                                             :frequent #{[1026 :vessel/imo "123"]
                                                         [1026 :vessel/name "Jekyll"]}}}

        tx-frequent-2 []

        tx-data-frequent-2 #{[:db/retract 1026 :vessel/name "Jekyll"]}

        db-after-frequent-2 {:schema schema
                             :refs refs
                             :source-datoms {:sporadic sporadic-1-datoms
                                             :frequent #{}}}

        tx-sporadic-2 [{:route/number "100"
                        :route/services [{:service/id :s567}]}]

        tx-data-sporadic-2 #{[:db/retract 1025 :service/allocated-vessel 1026]
                             [:db/retract 1026 :vessel/imo "123"]}

        db-after-sporadic-2 {:schema schema
                             :refs refs
                             :source-datoms {:sporadic #{[1024 :route/number "100"]
                                                         [1024 :route/services 1025]
                                                         [1025 :service/id :s567]}
                                             :frequent #{}}}]
    (is (= (-> (sut/with db-at-first :sporadic tx-sporadic-1)
               (update :tx-data set))
           {:tx-data tx-data-sporadic-1
            :db-before db-at-first
            :db-after db-after-sporadic-1}))

    (is (= (-> (sut/with db-after-sporadic-1 :frequent tx-frequent-1)
               (update :tx-data set))
           {:tx-data tx-data-frequent-1
            :db-before db-after-sporadic-1
            :db-after db-after-frequent-1}))

    (is (= (-> (sut/with db-after-frequent-1 :frequent tx-frequent-2)
               (update :tx-data set))
           {:tx-data tx-data-frequent-2
            :db-before db-after-frequent-1
            :db-after db-after-frequent-2}))

    (is (= (-> (sut/with db-after-frequent-2 :sporadic tx-sporadic-2)
               (update :tx-data set))
           {:tx-data tx-data-sporadic-2
            :db-before db-after-frequent-2
            :db-after db-after-sporadic-2}))))
