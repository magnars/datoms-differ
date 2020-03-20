(ns datoms-differ.core
  "Deprecation warning: This namespace is here for backwards compatibility. You
  probably want to use datoms-differ.api instead."
  (:require [clojure.set :as set]
            [datoms-differ.impl.core-helpers :as ch]))

(def default-db-id-partition
  {:from 0x10000000
   :to   0x1FFFFFFF})

(defn create-refs-lookup [{:keys [from to]} old-refs all-refs]
  (let [lowest-new-eid (inc (apply max
                                   (dec from)
                                   (filter #(<= from % to) (vals old-refs))))]
    (->> all-refs
         (remove old-refs)
         (map-indexed (fn [i ref]
                        (if (= (first ref) :db/id)
                          (let [eid (second ref)]
                            (when (<= from eid to)
                              (throw (ex-info "Asserted :db/id cannot be within the internal db-id-partition, check :datoms-differ.core/db-id-partition" {:ref ref :internal-partition {:from from :to to}})))
                            [ref (second ref)])
                          (let [eid (+ lowest-new-eid i)]
                            (when-not (<= from eid to)
                              (throw (ex-info "Generated internal eid falls outside internal db-id-partition, check :datoms-differ.core/db-id-partition" {:ref ref :eid eid :internal-partition {:from from :to to}})))
                            [ref eid]))))
         (into old-refs))))

(defn flatten-entity-map [{:keys [ref? many? component?] :as attrs} refs entity]
  (let [eid (refs (ch/get-entity-ref attrs entity))
        disallow-nils (fn [k v]
                        (when (nil? v)
                          (throw (ex-info "Attributes cannot be nil" {:entity (ch/get-entity-ref attrs entity)
                                                                      :key k}))))]
    (mapcat (fn [[k v]]
              (cond
                (= k :db/id)
                nil ;; db/id is not an attribute so exclude it

                (ref? k)
                (doall
                 (for [v (if (many? k) v [v])]
                   (do
                     (disallow-nils k v)
                     [eid k (if (number? v)
                              v
                              (refs (ch/get-entity-ref attrs v)))])))

                (ch/reverse-ref? k)
                (let [reverse-k (ch/reverse-ref-attr k)]
                  (for [ref-entity-map (if (component? reverse-k) [v] v)]
                    [(refs (ch/get-entity-ref attrs ref-entity-map)) reverse-k eid]))

                :else-scalar
                (doall
                 (for [v (if (many? k) v [v])]
                   (do
                     (disallow-nils k v)
                     [eid k v])))))
            entity)))

(defn disallow-conflicting-values [refs {:keys [many?]} datoms]
  (doseq [[[e a] datoms] (group-by #(take 2 %) datoms)]
    (when (and (not (many? a))
               (< 1 (count datoms)))
      (throw (ex-info "Conflicting values asserted for entity"
                      (let [e->entity-ref (set/map-invert refs)]
                        {:entity-ref (e->entity-ref e)
                         :attr a
                         :conflicting-values (into #{} (map #(nth % 2) datoms))}))))))

(defn disallow-empty-entities [all-entities datoms refs]
  (let [entity-id-has-datoms? (set (map first datoms))
        entity-id-is-known-ref? (set (map second refs))]
    (doseq [e all-entities]
      (when (and (empty? (dissoc e :db/id))
                 (not (entity-id-has-datoms? (:db/id e)))
                 (not (entity-id-is-known-ref? (:db/id e))))
        (throw (ex-info (str "No attributes asserted for entity: " (pr-str e)) {}))))))

(defn explode [{:keys [schema refs]} entity-maps]
  (let [attrs (ch/find-attrs schema)
        all-entities (ch/find-all-entities attrs entity-maps)
        entity-refs (distinct (map #(ch/get-entity-ref attrs %) all-entities))
        new-refs (create-refs-lookup (::db-id-partition schema default-db-id-partition) refs entity-refs)
        datoms (set (mapcat #(flatten-entity-map attrs new-refs %) all-entities))]
    (disallow-conflicting-values new-refs attrs datoms)
    (disallow-empty-entities all-entities datoms refs)
    {:refs new-refs
     :datoms datoms}))

(defn diff [datoms-before datoms-after]
  (let [new (set/difference datoms-after datoms-before)
        old (set/difference datoms-before datoms-after)]
    (concat
     (for [[e a v] old] [:db/retract e a v])
     (for [[e a v] new] [:db/add e a v]))))

(defn empty-db [schema]
  {:schema schema
   :refs {}
   :source-datoms {}})

(defn create-conn [schema]
  (atom (empty-db schema)))

(defn get-datoms [db]
  (apply set/union #{} (vals (:source-datoms db))))

(defn disallow-conflicting-sources [db]
  (let [{:keys [many? ref?]} (ch/find-attrs (:schema db))]
    (doseq [[[e a] datoms] (->> (:source-datoms db)
                                (mapcat (fn [[source datoms]]
                                          (keep (fn [[e a v]]
                                                  (when-not (many? a)
                                                    {:e e :a a :v v :source source}))
                                                datoms)))
                                (group-by (fn [{:keys [e a]}] [e a])))]
      (when (< 1 (count (set (map :v datoms))))
        (let [e->entity-ref (set/map-invert (:refs db))]
          (throw
           (ex-info "Conflicting values asserted between sources"
                    (into {}
                          (for [{:keys [e a v source]} datoms]
                            [source [(e->entity-ref e) a (if (ref? a)
                                                           (e->entity-ref v)
                                                           v)]])))))))))

(defn- update-db-with-source-entity-maps [db source entity-maps]
  (let [{:keys [datoms refs]} (explode db entity-maps)]
    (if (-> entity-maps meta :partial-update?)
      (update-in (assoc db :refs refs) [:source-datoms source] #(into % datoms))
      (assoc-in (assoc db :refs refs) [:source-datoms source] datoms))))

(defn with [db source entity-maps]
  (let [db-after (update-db-with-source-entity-maps db source entity-maps)
        _ (disallow-conflicting-sources db-after)]
    {:tx-data (diff (get-datoms db) (get-datoms db-after))
     :db-before db
     :db-after db-after}))

(defn with-sources [db source->entity-maps]
  (let [db-after (reduce (fn [db [source entity-maps]]
                           (update-db-with-source-entity-maps db source entity-maps))
                         db
                         source->entity-maps)
        _ (disallow-conflicting-sources db-after)]
    {:tx-data (diff (get-datoms db) (get-datoms db-after))
     :db-before db
     :db-after db-after}))

(defn transact! [conn source entity-maps]
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (with db source entity-maps)]
                    (reset! report r)
                    (:db-after r))))
    @report))

(defn transact-sources! [conn source->entity-maps]
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (with-sources db source->entity-maps)]
                    (reset! report r)
                    (:db-after r))))
    @report))
