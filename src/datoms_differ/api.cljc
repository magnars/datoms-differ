(ns datoms-differ.api
  (:require [datoms-differ.datom :as d]
            [datoms-differ.export :as dd-export]
            [datoms-differ.impl.core-helpers :as ch]
            [me.tonsky.persistent-sorted-set :as set]
            [medley.core :refer [map-vals]])
  (:import [datoms_differ.datom Datom]))

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

(defn- get-lowest-new-eid [{:keys [from to]} eavs]
  (inc (max
        (dec from)
        (or (-> eavs (set/rslice nil nil) first :e) 0))))

(defn- create-refs-lookup [{:keys [schema attrs eavs refs]} entities]
  (let [{:keys [from to] :as db-id-partition} (::db-id-partition schema default-db-id-partition)
        lowest-new-eid (get-lowest-new-eid db-id-partition eavs)]
      (loop [idx lowest-new-eid
             new-refs (transient refs)
             entities-with-eid (transient [])
             entities entities]
        (let [entity (first entities)
              rf (when entity (ch/get-entity-ref attrs entity))
              eid (when rf (new-refs rf))]
          (cond
            (nil? entity) [(persistent! new-refs) (persistent! entities-with-eid)]

            eid (recur idx
                       new-refs
                       (conj! entities-with-eid [entity eid])
                       (next entities))

            :else
            (do
              (when-not (<= from idx to)
                (throw (ex-info "Generated internal eid falls outside internal db-id-partition, check :datoms-differ.core2/db-id-partition"
                                {:ref rf :eid idx :internal-partition {:from from :to to}})))
              (recur (inc idx)
                     (assoc! new-refs rf idx)
                     (conj! entities-with-eid [entity idx])
                     (next entities))))))))

(defn- flatten-all-entities [source {:keys [ref? many? component?] :as attrs} refs all-entities]
  (let [disallow-nils (fn [k v entity]
                        (when (nil? v)
                          (throw (ex-info "Attributes cannot be nil" {:entity (ch/get-entity-ref attrs entity)
                                                                      :key k}))))
        get-eid #(refs (ch/get-entity-ref-unsafe attrs %))]
    (->> all-entities
         (reduce
          (fn [acc [entity eid]]
            (reduce-kv
             (fn [acc k v]
               (cond
                 (ref? k)
                 (reduce (fn [acc v]
                           (disallow-nils k v entity)
                           (conj! acc (d/datom eid k (if (number? v) v (get-eid v)) source)))
                         acc
                         (if (many? k) v [v]))

                 (ch/reverse-ref? k)
                 (let [reverse-k (ch/reverse-ref-attr k)]
                   (reduce (fn [acc ref-entity-map]
                             (conj! acc (d/datom (get-eid ref-entity-map) reverse-k eid source)))
                           acc
                           (if (component? reverse-k) [v] v)))

                 :else-scalar
                 (reduce (fn [acc v]
                           (disallow-nils k v entity)
                           (conj! acc (d/datom eid k v source)))
                         acc
                         (if (many? k) v [v]))))
             acc
             entity))
          (transient []))
         persistent!
         d/to-eavs)))

(defn- explode-entity-maps [source {:keys [schema attrs] :as db} entity-maps]
  (let [all-entities (ch/find-all-entities attrs entity-maps)
        [new-refs all-entities-with-eid] (create-refs-lookup db all-entities)
        datoms (flatten-all-entities source attrs new-refs all-entities-with-eid)]
    {:refs new-refs
     :datoms datoms}))

(defn- find-conflicting-value [many? eavs]
  (:conflict
   (persistent!
    (reduce
     (fn [{:keys [prev] :as acc} ^Datom curr]
       (cond
         (many? (.-a curr))
         acc

         (nil? prev)
         (assoc! acc :prev curr)

         (d/diff-in-value? prev curr)
         (reduced (assoc! acc :conflict [(:e curr) (:a curr)]))

         :else (assoc! acc :prev curr)))
     (transient {})
     eavs))))

(defn- find-conflicting-value-by-additions [many? eavs to-add]
  (reduce
   (fn [acc ^Datom d]
     (if (many? (.-a d))
       acc
       (let [search-d (d/datom (.-e d) (.-a d) nil nil)
             datoms (set/slice eavs search-d search-d)]
         (if (find-conflicting-value many? datoms)
           (reduced [(.-e d) (.-a d)])
           acc))))
   nil
   to-add))

(defn- throw-conflicting-values
  [{:keys [attrs eavs refs]} [e a]]
  (let [{:keys [ref?]} attrs
        e->entity-ref (clojure.set/map-invert refs)
        datoms (set/slice eavs (d/datom e a nil nil) (d/datom e a nil nil))
        v-fn (if (ref? a) (comp e->entity-ref :v) :v)]
    (throw
     (ex-info "Conflicting values asserted for entity"
              {:attr a
               :entity-ref (e->entity-ref e)
               :conflict (->> datoms
                              (group-by :s)
                              (map-vals #(->> % (map v-fn) set)))}))))

(defn- disallow-conflicts
  "Checks for conflicting values for all [e a]'s in db. Both across sources and for any given source.
   If a conflict is found it throws with info about source(s) and which values are conflicting."
  [{:keys [added eavs attrs] :as db}]
  (let [{:keys [many?]} attrs]
    ;; When there are few additions compared to total number of additions it's faster to just check the addition datoms
    (some->> (if (> (/ 5 100) (/ (count added) (max (count eavs) 1)))
               (find-conflicting-value-by-additions many? eavs added)
               (find-conflicting-value many? eavs))
             (throw-conflicting-values db))))

(defn- find-source-diffs [source eavs datoms]
  (diff-sorted (filter (partial d/source-equals? source) eavs)
               datoms
               d/cmp-datoms-eav-only))

(defn- update-eavs-by-diff
  "Fast path for updating the eavs index based on the results from a diff"
  [eavs [retracted added]]
  (let [transient-union (fn [s1 v2]
                          (if (< (count s1) (count v2))
                            (persistent! (reduce conj! (transient (d/to-eavs v2)) s1))
                            (persistent! (reduce conj! s1 v2))))]
    (loop [eavs-t (transient eavs)
           retracted retracted
           added added]
      (let [r (first retracted)
            a (first added)]
        (cond
          (and r a) (recur (-> eavs-t (disj! r) (conj! a)) (next retracted) (next added))
          (and (nil? r) a) (transient-union eavs-t added)
          (and r (nil? a)) (persistent! (reduce disj! eavs-t retracted))
          :else (persistent! eavs-t))))))

(defn- prune-refs
  "Remove refs that are no longer present in eavs."
  [{:keys [attrs retracted eavs refs]}]
  (let [{:keys [identity?]} attrs]
    (cond
      (= 0 (count retracted))
      refs

      (> (/ 1 6) (/ (count retracted) (max (count eavs) 1)))
      (persistent!
       (reduce (fn [acc [e a v]]
                 (if (and (identity? a)
                          (not (set/slice eavs (d/datom e a nil nil) (d/datom e a nil nil))))
                   (dissoc! acc [a v])
                   acc))
               (transient refs)
               retracted))

      :else
      (persistent!
       (reduce (fn [acc ^Datom d]
                 (if (identity? (.-a d))
                   (assoc! acc [(.-a d) (.-v d)] (.-e d))
                   acc))
               (transient {})
               eavs)))))

(defn- create-tx-data
  "Creates datomic transaction data from sorted sequences of add and removed calculated for each source
   Due to potential presence of same datom present in multiple sources,
   some additional filtering is needed prior to generating the final tx report data"
  [{:keys [retracted added eavs]}]
  (concat
   (persistent!
    (reduce (fn [acc ^Datom d]
              (if (d/contains-eav? eavs d)
                acc
                (conj! acc [:db/retract (.-e d) (.-a d) (.-v d)])))
            (transient [])
            retracted))
   (for [^Datom d (d/to-eav-only added)]
     [:db/add (.-e d) (.-a d) (.-v d)])))

(defn with-sources [db source->entity-maps]
  (let [{:keys [many?] :as attrs} (ch/find-attrs (:schema db))
        update-refs #(assoc % :refs (prune-refs %))
        db-after (->> source->entity-maps
                      (reduce (fn [db [source entity-maps]]
                                (when-not (keyword? source)
                                  (throw (ex-info "Source must be a keyword." {:source source})))
                                (let [{:keys [datoms refs]} (explode-entity-maps source db entity-maps)
                                      [retracted added] (find-source-diffs source (:eavs db) datoms)]
                                  (-> db
                                      (update :retracted into retracted)
                                      (update :added into added)
                                      (assoc :refs refs)
                                      (update :eavs update-eavs-by-diff [retracted added]))))
                              (assoc db :retracted [] :added [] :attrs attrs))
                      update-refs)
        _ (disallow-conflicts db-after)]
    {:tx-data (create-tx-data db-after)
     :db-before db
     :db-after (dissoc db-after :attrs :added :retracted)}))

(defn with [db source entity-maps]
  (with-sources db {source entity-maps}))

(defn- empty-db [schema]
  {:schema schema
   :refs {}
   :eavs (d/empty-eavs)})

(defn create-conn
  "Takes a datascript schema and creates a 'connection' (really, an atom with an empty db)"
  [schema]
  (atom (empty-db schema)))

(defn transact-sources!
  "Takes a connection and a map from source identifier to a list of entity maps, and transacts them all into the connection."
  [conn source->entity-maps]
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (with-sources db source->entity-maps)]
                    (reset! report r)
                    (:db-after r))))
    @report))

(defn transact!
  "Takes a connection, a keyword source identifier and a list of entity maps, and transacts them into the connection."
  [conn source entity-maps]
  (transact-sources! conn {source entity-maps}))

;; Convenience functions

(defn explode
  "Given a schema and a list of entity-maps, you get a map of datoms (with generated entity ids) and a map of lookup-refs->eid back"
  [schema entity-maps]
  (let [{:keys [many? ref?] :as attrs} (ch/find-attrs schema)
        {:keys [refs datoms] :as res} (explode-entity-maps ::ignore
                                                           {:schema schema
                                                            :attrs attrs
                                                            :eavs (d/empty-eavs)
                                                            :refs {}}
                                                           entity-maps)]

    (when-let [[e a] (find-conflicting-value many? datoms)]
      (let [e->entity-ref (clojure.set/map-invert refs)
              conflicting-datoms (set/slice datoms (d/datom e a nil nil) (d/datom e a nil nil))
              v-fn (if (ref? a) (comp e->entity-ref :v) :v)]
          (throw (ex-info "Conflicting values asserted for entity"
                          {:entity-ref (e->entity-ref e)
                           :attr a
                           :conflicting-values (into #{} (map v-fn conflicting-datoms))}))))

    (update res :datoms #(map (fn [[e a v]] [e a v]) %))))

(defn get-datoms
  "Get all datoms `[e a v]` in given database (across sources)"
  [db]
  (->> (:eavs db)
       (d/to-eav-only)
       (map (fn [^Datom d] [(.-e d) (.-a d) (.-v d)]))))

(defn export-db
  "Export database to DataScript. Gives you a string that can be read by clojurescript (when datascript is loaded) to create a datascript db."
  [{:keys [schema] :as db}]
  (dd-export/export (dd-export/prep-for-datascript schema)
                    (get-datoms db)
                    :start-tx (inc (:to default-db-id-partition))
                    :partition-key ::db-id-partition))

(def
  ^{:doc "Remove retractions of values that are later asserted in tx-data"
    :arglists '([schema tx-data])}
  prune-diffs dd-export/prune-diffs)
