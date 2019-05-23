(ns datoms-differ.core
  (:require [clojure.set :as set]))

(def default-db-id-partition
  {:from 8796093022208
   :to   8806093022208}) ;; 10,000,000,000 entity ids should be enough for anybody™

(defn find-identity-attrs [schema]
  (set (keep (fn [[k v]]
               (when (= :db.unique/identity (:db/unique v))
                 k))
             schema)))

(defn find-attr [schema target-k target-v]
  (set (keep (fn [[k v]]
               (when (= target-v (target-k v))
                 k))
             schema)))

(defn find-attrs [schema]
  {:identity? (find-attr schema :db/unique :db.unique/identity)
   :ref? (find-attr schema :db/valueType :db.type/ref)
   :many? (find-attr schema :db/cardinality :db.cardinality/many)
   :component? (find-attr schema :db/isComponent true)})

(defn get-entity-ref [attrs entity]
  (let [refs (select-keys entity (conj (:identity? attrs) :db/id))]
    (case (bounded-count 2 refs)
      0 (throw (ex-info "Entity without identity attribute"
                        {:entity entity
                         :attrs (:identity attrs)}))
      1 (first refs)
      2 (throw (ex-info (str "Entity with multiple identity attributes: " refs)
                        {:entity entity
                         :attrs (:identity attrs)})))))

(defn reverse-ref? [k]
  (= \_ (first (name k))))

(defn reverse-ref-attr [k]
  (if (reverse-ref? k)
    (keyword (namespace k) (subs (name k) 1))
    (keyword (namespace k) (str "_" (name k)))))

(defn find-all-entities [{:keys [ref? many? component?] :as attrs} entity-maps]
  (->> (mapcat seq entity-maps)
       (mapcat (fn [[k v]]
                 (cond
                   (ref? k) (find-all-entities attrs (if (many? k) v [v]))
                   (reverse-ref? k) (let [reverse-k (reverse-ref-attr k)]
                                      (find-all-entities attrs (if (component? reverse-k) [v] v))))))
       (into entity-maps)))

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
  (let [eid (refs (get-entity-ref attrs entity))
        disallow-nils (fn [k v]
                        (when (nil? v)
                          (throw (ex-info "Attributes cannot be nil" {:entity (get-entity-ref attrs entity)
                                                                      :key k}))))]
    (mapcat (fn [[k v]]
              (cond
                (= k :db/id)
                nil ;; db/id is not an attribute so exclude it

                (ref? k)
                (for [v (if (many? k) v [v])]
                  (do
                    (disallow-nils k v)
                    [eid k (if (number? v)
                             v
                             (refs (get-entity-ref attrs v)))]))

                (reverse-ref? k)
                (let [reverse-k (reverse-ref-attr k)]
                  (for [ref-entity-map (if (component? reverse-k) [v] v)]
                    [(refs (get-entity-ref attrs ref-entity-map)) reverse-k eid]))

                :else-scalar
                (do
                  (disallow-nils k v)
                  [[eid k v]])))
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
  (let [attrs (find-attrs schema)
        all-entities (find-all-entities attrs entity-maps)
        entity-refs (distinct (map #(get-entity-ref attrs %) all-entities))
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
  (let [{:keys [many? ref?]} (find-attrs (:schema db))]
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
