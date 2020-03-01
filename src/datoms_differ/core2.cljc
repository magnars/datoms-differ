(ns datoms-differ.core2
  (:require [datoms-differ.datom :as d]
            [me.tonsky.persistent-sorted-set :as set]))

(defn- diff-sorted [a b cmp]
  (loop [only-a (transient [])
         only-b (transient [])
         a a
         b b]
    (cond
      (empty? a) [(persistent! only-a) (into (persistent! only-b) b)]
      (empty? b) [(into (persistent! only-a) a) (persistent! only-b)]
      :else
      (let [first-a (first a)
            first-b (first b)
            diff (cmp first-a first-b)]
        (cond
          (== diff 0) (recur only-a only-b (next a) (next b))
          (< diff 0)  (recur (conj! only-a first-a) only-b (next a) b)
          (> diff 0)  (recur only-a (conj! only-b first-b) a (next b)))))))

(def default-db-id-partition
  {:from 0x10000000
   :to   0x1FFFFFFF})

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
  (let [refs (select-keys entity (:identity? attrs))]
    (case (bounded-count 2 refs)
      0 (throw (ex-info "Entity without identity attribute"
                        {:entity entity
                         :attrs (:identity attrs)}))
      1 (first refs)
      2 (throw (ex-info (str "Entity with multiple identity attributes: " refs)
                        {:entity entity
                         :attrs (:identity attrs)})))))

(defn reverse-ref? [k]
  (.startsWith (name k) "_"))

(defn reverse-ref-attr [k]
  (if (reverse-ref? k)
    (keyword (namespace k) (subs (name k) 1))
    (keyword (namespace k) (str "_" (name k)))))

(defn find-all-entities [{:keys [ref? many? component?] :as attrs} entity-maps]
  (persistent!
   (reduce
    (fn [res entity]
      (reduce-kv
       (fn [res k v]
         (reduce conj! res
                 (cond
                   (ref? k) (find-all-entities attrs (if (many? k) v [v]))
                   (reverse-ref? k) (let [reverse-k (reverse-ref-attr k)]
                                      (find-all-entities attrs (if (component? reverse-k) [v] v)))
                   :else [])))
       res
       entity))
    (transient (into [] entity-maps))
    entity-maps)))

(defn get-lowest-new-eid [{:keys [from to]} eavs]
  (inc (max
        (dec from)
        (or (-> eavs (set/rslice nil nil) first :e) 0))))

(defn create-refs-lookup [attrs lowest-new-eid {:keys [from to]} old-refs entities]
  (let [xform (comp
               (map #(get-entity-ref attrs %))
               (distinct)
               (remove old-refs)
               (map-indexed (fn [i ref]
                              (let [eid (+ lowest-new-eid i)]
                                (when-not (<= from eid to)
                                  (throw (ex-info "Generated internal eid falls outside internal db-id-partition, check :datoms-differ.core2/db-id-partition"
                                                  {:ref ref :eid eid :internal-partition {:from from :to to}})))
                                [ref eid]))))]
    (into old-refs xform entities)))

(defn flatten-all-entities [source {:keys [ref? many? component?] :as attrs} refs all-entities]
  (let [disallow-nils (fn [k v entity]
                        (when (nil? v)
                          (throw (ex-info "Attributes cannot be nil" {:entity (get-entity-ref attrs entity)
                                                                      :key k}))))]
    (->> all-entities
         (reduce
          (fn [acc entity]
            (let [eid (refs (get-entity-ref attrs entity))]
              (reduce-kv
               (fn [acc k v]
                 (cond
                   (ref? k)
                   (reduce (fn [acc v]
                             (disallow-nils k v entity)
                             (conj! acc (d/datom eid k (if (number? v) v (refs (get-entity-ref attrs v))) source)))
                           acc
                           (if (many? k) v [v]))

                   (reverse-ref? k)
                   (let [reverse-k (reverse-ref-attr k)]
                     (reduce (fn [acc ref-entity-map]
                               (conj! acc (d/datom (refs (get-entity-ref attrs ref-entity-map)) reverse-k eid source)))
                             acc
                             (if (component? reverse-k) [v] v)))

                   :else-scalar
                   (reduce (fn [acc v]
                             (disallow-nils k v entity)
                             (conj! acc (d/datom eid k v source)))
                           acc
                           (if (many? k) v [v]))))
               acc
               entity)))
          (transient []))
         persistent!
         d/to-eavs)))

(defn find-conflicting-value [many? eavs]
  (:conflict
   (persistent!
    (reduce
     (fn [{:keys [prev] :as acc} {:keys [e a v] :as curr}]
       (cond
         (many? a)
         acc

         (nil? prev)
         (assoc! acc :prev curr)

         (and (= (:e prev) e)
              (= (:a prev) a)
              (not= (:v prev) v))
         (reduced (assoc! acc :conflict [e a]))

         :else (assoc! acc :prev curr)))
     (transient {})
     eavs))))

(defn disallow-conflicting-values [refs {:keys [many?]} datoms]
  (when-let [[e a] (find-conflicting-value many? datoms)]
    (let [datoms (set/slice datoms (d/datom e a nil nil) (d/datom e a nil nil))]
      (throw (ex-info "Conflicting values asserted for entity"
                      (let [e->entity-ref (clojure.set/map-invert refs)]
                        {:entity-ref (e->entity-ref e)
                         :attr a
                         :conflicting-values (into #{} (map :v datoms))}))))))

(defn explode [source {:keys [schema attrs refs eavs]} entity-maps]
  (let [db-id-partition (::db-id-partition schema default-db-id-partition)
        lowest-new-eid (get-lowest-new-eid db-id-partition eavs)
        all-entities (find-all-entities attrs entity-maps)
        new-refs (create-refs-lookup attrs lowest-new-eid db-id-partition refs all-entities)
        datoms (flatten-all-entities source attrs new-refs all-entities)]
    (disallow-conflicting-values new-refs attrs datoms)
    {:refs new-refs
     :datoms datoms}))

(defn diff [datoms-before datoms-after]
  (let [[old new] (diff-sorted datoms-before datoms-after d/cmp-datoms-eav-only)]
    (concat
     (for [[e a v] old] [:db/retract e a v])
     (for [[e a v] new] [:db/add e a v]))))

(defn disallow-conflicting-sources [{:keys [attrs eavs refs]}]
  (let [{:keys [many? ref?]} attrs
        e->entity-ref (clojure.set/map-invert refs)]
    (when-let [[e a] (find-conflicting-value many? eavs)]
      (let [datoms (set/slice eavs (d/datom e a nil nil) (d/datom e a nil nil))]
        (throw
         (ex-info "Conflicting values asserted between sources"
                  (into {}
                        (for [{:keys [e a v s]} datoms]
                          [s [(e->entity-ref e) a (if (ref? a)
                                                    (e->entity-ref v)
                                                    v)]]))))))))

(defn with-sources [db source->entity-maps]
  (let [db-after (reduce (fn [db [source entity-maps]]
                           (let [{:keys [datoms refs]} (explode source db entity-maps)]
                             (-> db
                                 (assoc :refs refs)
                                 (update :eavs clojure.set/union datoms))))
                         db
                         source->entity-maps)
        _ (disallow-conflicting-sources db-after)]
    {:tx-data (diff (:eavs db) (:eavs db-after))
     :db-before db
     :db-after db-after}))


;; TODO: Public API, maybe have internals in separate ns?

(defn empty-db [schema]
  {:schema schema
   :attrs (find-attrs schema)
   :refs {}
   :eavs (d/empty-eavs)})

(defn create-conn [schema]
  (atom (empty-db schema)))

(defn transact-sources! [conn source->entity-maps]
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (with-sources db source->entity-maps)]
                    (reset! report r)
                    (:db-after r))))
    @report))

