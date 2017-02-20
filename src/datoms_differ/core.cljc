(ns datoms-differ.core
  (:require [clojure.set :as set]))

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
   :ref?      (find-attr schema :db/valueType :db.type/ref)
   :many?     (find-attr schema :db/cardinality :db.cardinality/many)})

(defn get-entity-ref [attrs entity]
  (let [refs (select-keys entity (:identity? attrs))]
    (case (bounded-count 2 refs)
      0 (throw (ex-info "Entity without identity attribute"
                        {:entity entity
                         :attrs (:identity attrs)}))
      1 (first refs)
      2 (throw (ex-info "Entity with multiple identity attributes"
                        {:entity entity
                         :attrs (:identity attrs)})))))

(defn find-all-entities [{:keys [ref? many?] :as attrs} entity-maps]
  (->> (mapcat seq entity-maps)
       (mapcat (fn [[k v]]
                 (when (ref? k)
                   (find-all-entities attrs (if (many? k) v [v])))))
       (into entity-maps)))

(defn create-refs-lookup [old-refs all-refs]
  (let [lowest-new-eid (inc (apply max 1023 (vals old-refs)))]
    (->> all-refs
         (remove old-refs)
         (map-indexed (fn [i ref] [ref (+ lowest-new-eid i)]))
         (into old-refs))))

(defn flatten-entity-map [{:keys [ref? many?] :as attrs} refs entity]
  (let [eid (refs (get-entity-ref attrs entity))
        disallow-nils (fn [k v]
                        (when (nil? v)
                          (throw (ex-info "Attributes cannot be nil" {:entity (get-entity-ref attrs entity)
                                                                      :key k}))))]
    (mapcat (fn [[k v]]
              (if (ref? k)
                (for [v (if (many? k) v [v])]
                  (do
                    (disallow-nils k v)
                    [eid k (refs (get-entity-ref attrs v))]))
                (do
                  (disallow-nils k v)
                  [[eid k v]])))
            entity)))

(defn explode [{:keys [schema refs]} entity-maps]
  (let [attrs (find-attrs schema)
        all-entities (find-all-entities attrs entity-maps)
        entity-refs (map #(get-entity-ref attrs %) all-entities)
        new-refs (create-refs-lookup refs entity-refs)]
    {:refs new-refs
     :datoms (set (mapcat #(flatten-entity-map attrs new-refs %) all-entities))}))

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

(defn with [db source entity-maps]
  (let [{:keys [datoms refs]} (explode db entity-maps)
        db-after (-> (assoc db :refs refs)
                     (assoc-in [:source-datoms source] datoms))]
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
