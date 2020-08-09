(ns datoms-differ.impl.core-helpers)

(defn- find-identity-attrs [schema]
  (set (keep (fn [[k v]]
               (when (= :db.unique/identity (:db/unique v))
                 k))
             schema)))

(defn- find-attr [schema target-k target-v]
  (set (keep (fn [[k v]]
               (when (= target-v (target-k v))
                 k))
             schema)))

(defn- validate-attrs [{:keys [many? ref? tuple?]}]
  (doseq [[k tupleAttrs] tuple?]
    (doseq [tupleAttr tupleAttrs]
      (when (many? tupleAttr)
        (throw (ex-info "Tuple attribute can't reference cardinality many attribute"
                        {:attr k
                         :conflict #{tupleAttr}})))
      (when (tuple? tupleAttr)
        (throw (ex-info "Tuple attribute can't reference another tuple attribute"
                        {:attr k
                         :conflict #{tupleAttr}}))))))

(defn find-attrs
  "Find relevant attrs grouped by attribute type"
  [schema]
  (let [attrs {:identity? (find-attr schema :db/unique :db.unique/identity)
               :ref? (find-attr schema :db/valueType :db.type/ref)
               :many? (find-attr schema :db/cardinality :db.cardinality/many)
               :component? (find-attr schema :db/isComponent true)
               :tuple? (into {} (keep (fn [[k v]]
                                        (when-let [tupleAttrs (:db/tupleAttrs v)]
                                          [k tupleAttrs]))
                                      schema))}]
    (validate-attrs attrs)
    attrs))

(defn add-tuple-attributes [attrs entity]
  (reduce-kv (fn [e k tupleAttrs]
               (if (some entity tupleAttrs)
                 (assoc e k (mapv #(get e %) tupleAttrs))
                 e))
          entity
          (:tuple? attrs)))

(defn get-entity-ref [attrs entity]
  (let [refs (select-keys entity (conj (:identity? attrs) :db/id))]
    (case (bounded-count 2 refs)
      0 (throw (ex-info "Entity without identity attribute"
                        {:entity entity
                         :attrs (:identity attrs)}))
      1 (first refs)
      2 (throw (ex-info (str "Entity with multiple identity attributes: " refs)
                        {:entity entity
                         :attrs (:identity? attrs)})))))

(defn- select-first-entry-of [map keyseq]
  (loop [ret nil keys (seq keyseq)]
    (when keys
      (if-let [entry (. clojure.lang.RT (find map (first keys)))]
        entry
        (recur ret (next keys))))))

(defn get-entity-ref-unsafe [attrs entity-map]
  (select-first-entry-of entity-map (:identity? attrs)))

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
