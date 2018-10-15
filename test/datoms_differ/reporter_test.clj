(ns datoms-differ.reporter-test
  (:require [clojure.test :refer [deftest is testing]]
            [datoms-differ.core-test :refer [schema]]
            [datoms-differ.reporter :as sut]))

(deftest replace-entity-ids-with-identifier
  (is (= (sut/replace-entity-ids-with-identifier schema [[1024 :route/number "100"]
                                                         [1024 :route/name "Stavanger-Tau"]
                                                         [1024 :route/services 1025]
                                                         [1025 :service/id :s567]])
         [[[:route/number "100"] :route/number "100"]
          [[:route/number "100"] :route/name "Stavanger-Tau"]
          [[:route/number "100"] :route/services [:service/id :s567]]
          [[:service/id :s567] :service/id :s567]])))

(deftest find-individual-changes
  (is (= (sut/find-individual-changes
          [[[:route/number "100"] :route/number "100"]
           [[:route/number "100"] :route/name "Stavanger-Tau"]]
          [[[:route/number "100"] :route/number "100"]])
         [[:removed [:route/number "100"] :route/name "Stavanger-Tau"]]))

  (is (= (sut/find-individual-changes
          [[[:route/number "100"] :route/number "100"]]
          [[[:route/number "100"] :route/number "100"]
           [[:route/number "100"] :route/name "Stavanger-Tau"]])
         [[:added [:route/number "100"] :route/name "Stavanger-Tau"]]))

  (is (= (sut/find-individual-changes
          [[[:route/number "100"] :route/services :service-1]
           [[:route/number "100"] :route/services :service-2]
           [[:route/number "100"] :route/services :service-3]]
          [[[:route/number "100"] :route/services :service-1]])
         [[:removed [:route/number "100"] :route/services :service-3]
          [:removed [:route/number "100"] :route/services :service-2]]))

  (is (= (sut/find-individual-changes
          [[[:route/number "100"] :route/number "100"]
           [[:route/number "100"] :route/name "Stavanger-Tau"]]
          [[[:route/number "100"] :route/number "100"]
           [[:route/number "100"] :route/name "Stavanger - Tau"]])
         [[:changed [:route/number "100"] :route/name ["Stavanger-Tau" "Stavanger - Tau"]]])))

(deftest group-changes
  (is (= (sut/group-changes
          [[:removed [:route/number "100"] :route/active? true]
           [:removed [:route/number "200"] :route/active? true]
           [:removed [:route/number "300"] :route/active? true]
           [:removed [:route/number "400"] :route/active? true]])
         [[:several-entities :removed :route/active? [[[:route/number "100"] true]
                                                      [[:route/number "200"] true]
                                                      [[:route/number "300"] true]
                                                      [[:route/number "400"] true]]]]))

  (is (= (sut/group-changes
          [[:changed [:route/number "100"] :route/name ["Stavanger-Tau" "Stavanger - Tau"]]
           [:removed [:route/number "100"] :route/active? true]
           [:added [:route/number "100"] :route/disabled? false]])
         [[:same-entity [:route/number "100"] [[:changed :route/name ["Stavanger-Tau" "Stavanger - Tau"]]
                                               [:removed :route/active? true]
                                               [:added :route/disabled? false]]]]))

  (testing "the largest grouping is chosen"
    (is (= (sut/group-changes
            [[:changed [:route/number "100"] :route/name ["Stavanger-Tau" "Stavanger - Tau"]]
             [:removed [:route/number "100"] :route/active? true]
             [:removed [:route/number "200"] :route/active? true]
             [:removed [:route/number "300"] :route/active? true]
             [:removed [:route/number "400"] :route/active? true]])
           [[:several-entities :removed :route/active? [[[:route/number "100"] true]
                                                        [[:route/number "200"] true]
                                                        [[:route/number "300"] true]
                                                        [[:route/number "400"] true]]]
            [:changed [:route/number "100"] :route/name ["Stavanger-Tau" "Stavanger - Tau"]]]))

    (is (= (sut/group-changes
            [[:changed [:route/number "100"] :route/name ["Stavanger-Tau" "Stavanger - Tau"]]
             [:removed [:route/number "100"] :route/active? true]
             [:added [:route/number "100"] :route/disabled? false]
             [:removed [:route/number "200"] :route/active? true]])
           [[:same-entity [:route/number "100"] [[:changed :route/name ["Stavanger-Tau" "Stavanger - Tau"]]
                                                 [:removed :route/active? true]
                                                 [:added :route/disabled? false]]]
            [:removed [:route/number "200"] :route/active? true]]))

    (is (= (sut/group-changes
            [[:changed [:route/number "100"] :route/name ["Stavanger-Tau" "Stavanger - Tau"]]
             [:added [:route/number "100"] :route/disabled? false]
             [:removed [:route/number "100"] :route/active? true]
             [:removed [:route/number "200"] :route/active? true]
             [:added [:route/number "200"] :route/disabled? false]
             [:removed [:route/number "300"] :route/active? true]
             [:removed [:route/number "400"] :route/active? true]])
           [[:several-entities :removed :route/active? [[[:route/number "100"] true]
                                                        [[:route/number "200"] true]
                                                        [[:route/number "300"] true]
                                                        [[:route/number "400"] true]]]
            [:same-entity [:route/number "100"] [[:changed :route/name ["Stavanger-Tau" "Stavanger - Tau"]]
                                                 [:added :route/disabled? false]]]
            [:added [:route/number "200"] :route/disabled? false]]))))

(deftest find-entire-entity-changes
  (is (= (sut/find-entire-entity-changes
          [[[:route/number "100"] :route/number "100"]
           [[:route/number "100"] :route/name "Stavanger-Tau"]]
          [[[:service/id :service123] :service/id :service123]
           [[:service/id :service123] :service/label "II"]
           [[:route/number "101"] :route/number "101"]
           [[:route/number "101"] :route/name "Fogn-Judaberg"]])
         [[:removed-entities :route/number [[[:route/number "100"] {:route/name "Stavanger-Tau"}]]]
          [:added-entities :route/number [[[:route/number "101"] {:route/name "Fogn-Judaberg"}]]]
          [:added-entities :service/id [[[:service/id :service123] {:service/label "II"}]]]]))

  (is (= (sut/find-entire-entity-changes
          [[[:route/number "100"] :route/number "100"]
           [[:route/number "100"] :route/name "Stavanger-Tau"]]
          [[[:route/number "101"] :route/number "101"]
           [[:route/number "101"] :route/name "Stavanger-Tau"]])
         [[:changed-identities [[[:route/number "100"] [:route/number "101"]]]]])))

(deftest find-changes
  (is (= (sut/find-changes
          [[[:route/number "100"] :route/number "100"]
           [[:route/number "100"] :route/name "Stavanger-Tau"]
           [[:route/number "200"] :route/number "200"]
           [[:route/number "200"] :route/name "Fogn-Judaberg"]
           [[:route/number "300"] :route/number "300"]
           [[:route/number "300"] :route/active? true]]
          [[[:route/number "101"] :route/number "101"]
           [[:route/number "101"] :route/name "Stavanger-Tau"]
           [[:route/number "200"] :route/number "200"]
           [[:route/number "200"] :route/name "Fogn-Jelsa-Judaberg"]
           [[:route/number "300"] :route/number "300"]
           [[:route/number "300"] :route/disabled? false]
           [[:service/id :service123] :service/id :service123]
           [[:service/id :service123] :service/label "II"]])
         [[:added-entities :service/id [[[:service/id :service123] #:service{:label "II"}]]]
          [:changed-identities [[[:route/number "100"] [:route/number "101"]]]]
          [:same-entity [:route/number "300"] [[:removed :route/active? true] [:added :route/disabled? false]]]
          [:changed [:route/number "200"] :route/name ["Fogn-Judaberg" "Fogn-Jelsa-Judaberg"]]])))

(deftest create-report
  (is (= (sut/create-report [[:changed [:route/number "200"] :route/name ["Fogn-Judaberg" "Fogn-Jelsa-Judaberg"]]])
         [{:text "Changed [:route/number \"200\"] :route/name to \"Fogn-Jelsa-Judaberg\""
           :details ["was \"Fogn-Judaberg\""]}]))

  (is (= (sut/create-report [[:changed [:route/number "200"] :route/numbers [(range 10) (range 100)]]])
         [{:text "Changed [:route/number \"200\"] :route/numbers to (0 1 2 3 4 5 6 7 8 9 10 11 12 13 14...)"
           :details ["was (0 1 2 3 4 5 6 7 8 9)"]}]))

  (is (= (sut/create-report [[:removed [:route/number "100"] :route/name "Stavanger-Tau"]])
         [{:text "Removed :route/name from [:route/number \"100\"]"
           :details ["was \"Stavanger-Tau\""]}]))

  (is (= (sut/create-report [[:added [:route/number "100"] :route/name "Stavanger-Tau"]])
         [{:text "Added :route/name \"Stavanger-Tau\" to [:route/number \"100\"]"}]))

  (is (= (sut/create-report [[:several-entities :removed :route/active? [[[:route/number "100"] true]
                                                                         [[:route/number "200"] true]
                                                                         [[:route/number "300"] false]
                                                                         [[:route/number "400"] false]
                                                                         [[:route/number "500"] false]
                                                                         [[:route/number "600"] false]]]])
         [{:text "Removed :route/active? from 6 entities"
           :details ["was true for 2× [:route/number]: \"100\" \"200\""
                     "was false for 4× [:route/number], e.g. \"300\" \"400\" \"500\""]}]))

  (is (= (sut/create-report [[:several-entities :added :some/details [[[:route/number "100"] 1]
                                                                      [[:route/number "200"] 2]
                                                                      [[:route/number "300"] 3]
                                                                      [[:route/number "400"] 4]
                                                                      [[:service/id :service-1] 5]
                                                                      [[:service/id :service-2] 6]
                                                                      [[:service/id :service-3] 7]]]])
         [{:text "Added :some/details to 7 entities"
           :details ["to 4× [:route/number], e.g. is 1 for \"100\""
                     "to 3× [:service/id], e.g. is 5 for :service-1"]}]))

  (is (= (sut/create-report [[:several-entities :changed :route/active? [[[:route/number "100"] [true false]]
                                                                         [[:route/number "200"] [true false]]
                                                                         [[:route/number "300"] [true false]]
                                                                         [[:route/number "400"] [true false]]
                                                                         [[:route/number "500"] [true false]]
                                                                         [[:route/number "600"] [true false]]]]])
         [{:text "Changed :route/active? for 6 entities"
           :details ["replaced [true false] for 6× [:route/number], e.g. \"100\" \"200\" \"300\""]}]))

  (is (= (sut/create-report [[:same-entity [:route/number "300"] [[:removed :route/active? true]
                                                                  [:changed :route/disabled? [false true]]
                                                                  [:added :route/details {:abcdefghi 123456 :defghijkl 456789 :ghijklmno 789123}]]]])
         [{:text "Changed 3 attributes for [:route/number \"300\"]"
           :details ["removed :route/active? true"
                     "changed :route/disabled? to true"
                     "added :route/details {:abcdefghi 123456, :defghijkl 4567...}"]}]))

  (is (= (sut/create-report [[:same-entity [:route/number "300"] [[:removed :route/active? true]
                                                                  [:removed :route/disabled? false]]]])
         [{:text "Removed 2 attributes from [:route/number \"300\"]"
           :details [":route/active? true"
                     ":route/disabled? false"]}]))

  (is (= (sut/create-report [[:changed-identities [[[:route/number 100] [:route/number "100"]]
                                                   [[:route/number 101] [:route/number "101"]]]]])
         [{:text "2 entities changed identity"
           :details ["[:route/number 100] to [:route/number \"100\"]"
                     "[:route/number 101] to [:route/number \"101\"]"]}]))

  (is (= (sut/create-report [[:added-entities :service/id [[[:service/id :service123] #:service{:label "II" :name "Foobar"}]]]])
         [{:text "Added entity [:service/id :service123]"
           :details [":service/label \"II\""
                     ":service/name \"Foobar\""]}]))

  (is (= (sut/create-report [[:added-entities :service/id [[[:service/id :service123] #:service{:label "I" :name "Foobar"}]
                                                           [[:service/id :service456] #:service{:label "II"}]]]])
         [{:text "Added 2× [:service/id] entities"
           :details [":service123 #:service{:label \"I\", :name \"Foobar\"}"
                     ":service456 #:service{:label \"II\"}"]}]))

  (is (= (sut/create-report [[:added-entities :route/number [[[:route/number 1] #:route{:name "One"}]
                                                             [[:route/number 2] #:route{:name "Two"}]
                                                             [[:route/number 3] #:route{:name "Three"}]
                                                             [[:route/number 4] #:route{:name "Four"}]
                                                             [[:route/number 5] #:route{:name "Five"}]
                                                             [[:route/number 6] #:route{:name "Six"}]]]])
         [{:text "Added 6× [:route/number] entities"
           :details ["1 #:route{:name \"One\"}"
                     "2 #:route{:name \"Two\"}"
                     "3 #:route{:name \"Three\"}"
                     "... and 3 more."]}]))

  (is (= (sut/create-report [[:removed-entities :route/number [[[:route/number 1] #:route{:name "One"}]
                                                               [[:route/number 2] #:route{:name "Two"}]
                                                               [[:route/number 3] #:route{:name "Three"}]
                                                               [[:route/number 4] #:route{:name "Four"}]
                                                               [[:route/number 5] #:route{:name "Five"}]
                                                               [[:route/number 6] #:route{:name "Six"}]]]])
         [{:text "Removed 6× [:route/number] entities"
           :details ["1 #:route{:name \"One\"}"
                     "2 #:route{:name \"Two\"}"
                     "3 #:route{:name \"Three\"}"
                     "... and 3 more."]}])))
