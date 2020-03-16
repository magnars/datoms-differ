(ns perf
  (:require [criterium.core :as crit]
            [datoms-differ.api :as c2]
            [datoms-differ.core :as c]
            [datoms-differ.datom :as d]
            [medley.core :ref [map-vals]]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set :as set]
            [taoensso.tufte :as tufte])
  (:import [datoms_differ.datom Datom]))

(comment
  (def history-schema
    {:vessel/imo {:spec string? :db/unique :db.unique/identity}
     :vessel/name {:spec string?}
     :vessel/type {}
     :vessel/dim-port {:spec int?}
     :vessel/dim-starboard {:spec int?}
     :vessel/dim-bow {:spec int?}
     :vessel/dim-stern {:spec int?}

     :position/lon {}
     :position/lat {}
     :position/inst {:db/unique :db.unique/identity :spec inst?}
     :position/speed {:spec number?}
     :position/heading {:spec int?}
     :position/rate-of-turn {:spec int?}
     :position/course {:spec number?}

     :entity/id {:spec string? :db/unique :db.unique/identity}
     :location/place-id {:spec string?}
     :location/lat {}
     :location/type {:spec keyword?}
     :location/lon {}
     :location/polygon {:spec vector? :compound-value? true}
     :location/name {:spec string?}

     :move/id {:spec string? :db/unique :db.unique/identity}
     :move/inst {:spec inst?}
     :move/imo {:spec string?}
     :move/location {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
     :move/kind {:spec keyword?}})

  ;; TODO: DOH, You'll have to try to get hold of your own test fixture for this for now (:
  (def history-entities
    (->> (slurp "/Users/mrundberget/projects/datoms-differ/history-entities.edn")
         (clojure.edn/read-string {})))

  (def modified-moves
    {:prepare-moves [#:move{:id "9855783-:departure-1582710092606-new", :imo "9855783", :location #:entity{:id "location/haugesund-1"}, :kind :departure, :inst #inst "2020-02-26T09:41:32.606-00:00"}
                     #:move{:id "9855783-:arrival-1582711626542-new", :imo "9855783", :location #:entity{:id "location/røvær-1"}, :kind :arrival, :inst #inst "2020-02-26T10:07:06.542-00:00"}
                     #:move{:id "9855783-:departure-1582713035833-new", :imo "9855783", :location #:entity{:id "location/røvær-1"}, :kind :departure, :inst #inst "2020-02-26T10:30:35.833-00:00"}]})


  ;; profiling
  (tufte/add-basic-println-handler!
   {:format-pstats-opts {:columns [:n-calls :p50 :p90 :mean :clock :total]
                         :format-id-fn name}})

  (tufte/profile {} (dotimes [i 50]
                      (tufte/p :transact (transact-for-existing))))


  (let [sample-db (c2/create-conn history-schema)]
    (println "\n********  NEW CLEAN DB  *********")
    (time (c2/transact-sources! sample-db history-entities))
    (println "\n********  NEW UPDATE  ***********")
    (time (c2/transact-sources! sample-db history-entities))
    (println "\n********  NEW TINY UPDATE  ***********")
    (time (c2/transact-sources! sample-db modified-moves))
    (count (:eavs @sample-db)))

  (let [sample-db (c/create-conn history-schema)]
    (println "\n********  OLD CLEAN DB  ********")
    (time (c/transact-sources! sample-db history-entities))
    (println "\n********  OLD UPDATE  ***********")
    (time (c/transact-sources! sample-db history-entities))
    (println "\n********  NEW TINY UPDATE  ***********")
    (time (c/transact-sources! sample-db modified-moves))
    nil)


  ;; ************* benchmarking **********************


  ;; NEW IMPL TESTS
  (def first-history-db
    (let [sample-db (c2/create-conn history-schema)]
      (c2/transact-sources! sample-db history-entities)
      sample-db))

  (defn transact-for-empty []
    (c2/transact-sources! (c2/create-conn history-schema) history-entities))

  (defn transact-for-existing []
    (let [db (atom @first-history-db)]
      (c2/transact-sources! db history-entities)))

  (defn transact-for-existing-small-change []
    (let [db (atom @first-history-db)]
      (c2/transact-sources! db modified-moves)))

  (def observations (->> (:prepare-observations history-entities) (drop 700) (take 100)))
  (defn transact-for-lots-off-add-and-remove []
    (let [db (atom @first-history-db)]
      (c2/transact-sources! db {:prepare-observations observations})))

  (crit/quick-bench (transact-for-empty))
  (crit/quick-bench (transact-for-existing))
  (crit/quick-bench (transact-for-existing-small-change))
  (crit/quick-bench (transact-for-lots-off-add-and-remove))

  (crit/with-progress-reporting (crit/bench (transact-for-empty)))
  (crit/with-progress-reporting (crit/bench (transact-for-existing)))
  (crit/with-progress-reporting (crit/bench (transact-for-existing-small-change)))


  ;; OLD IMPL TESTS
  (def first-history-db-old
    (let [sample-db (c/create-conn history-schema)]
      (c/transact-sources! sample-db history-entities)
      sample-db))

  (defn transact-for-empty-old []
    (c/transact-sources! (c/create-conn history-schema) history-entities))

  (defn transact-for-existing-old []
    (let [db (atom @first-history-db-old)]
      (c/transact-sources! db history-entities)))

  (defn transact-for-existing-old-small-change []
    (let [db (atom @first-history-db-old)]
      (c/transact-sources! db modified-moves)))

  (crit/with-progress-reporting (crit/bench (transact-for-empty-old)))
  (crit/with-progress-reporting (crit/bench (transact-for-existing-old)))
  (crit/with-progress-reporting (crit/bench (transact-for-existing-old-small-change)))

  )

(comment
  (def sample-datoms
    [(d/datom 536870912 :vessel/imo "1234" :prepare-vessels)
     (d/datom 536870912 :vessel/name "Fjordnerd" :prepare-vessels)
     (d/datom 536870912 :vessel/type :mf :prepare-vessels)
     (d/datom 536870913 :vessel/imo "5678" :prepare-vessels)
     (d/datom 536870913 :vessel/name "Limasol" :prepare-vessels)
     (d/datom 536870913 :vessel/type :ms :prepare-vessels)
     (d/datom 536870912 :vessel/imo "1234" :prepare-observations)
     (d/datom 536870912 :vessel/lat 60 :prepare-observations)
     (d/datom 536870912 :vessel/lon 59 :prepare-observations)
     ;; conflict
     (d/datom 536870912 :vessel/imo "12345" :prepare-infos)
     ;; dupe
     (d/datom 536870912 :vessel/imo "1234" :prepare-vessels)])

  (d/to-eavs sample-datoms)


  ;; TODO: Test custom compare using protocol
  (defn compare-map [x y]
    (if (map? y)
      (compare (hash x) (hash y))
      (throw (Exception. (str "Cannot compare " x " to " y)))))

  (defn compare-set [x y]
    (if (set? y)
      (compare (hash x) (hash y))
      (throw (Exception. (str "Cannot compare " x " to " y)))))

  (extend-protocol d/DatomValueComparator
    clojure.lang.PersistentArrayMap
    (customCompareTo [x y] (compare-map x y))

    clojure.lang.PersistentTreeMap
    (customCompareTo [x y] (compare-map x y))

    clojure.lang.PersistentHashMap
    (customCompareTo [x y] (compare-map x y))

    clojure.lang.PersistentHashSet
    (customCompareTo [x y] (compare-set x y)))

  (d/cmp-datoms-eav-only (d/datom 1 :vessel/imo {:dill/dall #{1}} :dummy)
                         (d/datom 1 :vessel/imo {:dill/dall #{}} :dammy))


  )
