(ns datoms-differ.datom
  (:require [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.arrays :as a]))

(defn combine-hashes [x y]
  #?(:clj  (clojure.lang.Util/hashCombine x y)
     :cljs (hash-combine x y)))

(declare hash-datom equiv-datom seq-datom nth-datom assoc-datom val-at-datom)

(deftype Datom [^int e a v s ^:unsynchronized-mutable ^int _hash]
  Object
  (hashCode [d]
    (if (zero? _hash)
      (let [h (int (hash-datom d))]
        (set! _hash h)
        h)
      _hash))
  (toString [d] (pr-str d))

  clojure.lang.Seqable
  (seq [d] (seq-datom d))

  clojure.lang.IPersistentCollection
  (equiv [d o] (and (instance? Datom o) (equiv-datom d o)))
  (empty [d] (throw (UnsupportedOperationException. "empty is not supported on Datom")))
  (count [d] 5)
  (cons [d [k v]] (assoc-datom d k v))

  clojure.lang.Indexed
  (nth [this i]           (nth-datom this i))
  (nth [this i not-found] (nth-datom this i not-found))

  clojure.lang.ILookup
  (valAt [d k] (val-at-datom d k nil))
  (valAt [d k nf] (val-at-datom d k nf))

  clojure.lang.Associative
  (entryAt [d k] (some->> (val-at-datom d k nil) (clojure.lang.MapEntry k)))
  (containsKey [e k] (#{:e :a :v :s} k))
  (assoc [d k v] (assoc-datom d k v)))


(defn ^Datom datom [e a v s] (Datom. e a v s 0))


(defn- hash-datom [^Datom d]
  (-> (hash (.-e d))
      (combine-hashes (hash (.-a d)))
      (combine-hashes (hash (.-v d)))
      (combine-hashes (hash (.-s d)))))

(defn- equiv-datom [^Datom d ^Datom o]
  (and (== (.-e d) (.-e o))
       (= (.-a d) (.-a o))
       (= (.-v d) (.-v o))
       (= (.-s d) (.-s o))))

(defn- seq-datom [^Datom d]
  (list (.-e d) (.-a d) (.-v d) (.-s d)))

(defn- val-at-datom [^Datom d k not-found]
  (case k
    :e      (.-e d)
    :a      (.-a d)
    :v      (.-v d)
    :s      (.-s d)
    not-found))

(defn- nth-datom
  ([^Datom d ^long i]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (.-s d)
     #?(:clj  (throw (IndexOutOfBoundsException.))
        :cljs (throw (js/Error. (str "Datom/-nth: Index out of bounds: " i))))))
  ([^Datom d ^long i not-found]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (.-s d)
     not-found)))

(defn- ^Datom assoc-datom [^Datom d k v]
  (case k
    :e     (datom v       (.-a d) (.-v d) (.-s d) )
    :a     (datom (.-e d) v       (.-v d) (.-s d) )
    :v     (datom (.-e d) (.-a d) v       (.-s d) )
    :s     (datom (.-e d) (.-a d) (.-v d) v            )
    (throw (IllegalArgumentException. (str "invalid key for Datom: " k)))))

#?(:clj
   (defmethod print-method Datom [^Datom d, ^java.io.Writer w]
     (.write w (str "#differ/Datom "))
     (binding [*out* w]
       (pr [(.-e d) (.-a d) (.-v d) (.-s d)]))))

(defn ^Datom datom-from-reader [vec]
  (apply datom vec))


;; Comparing datoms
#?(:clj
   (defmacro combine-cmp [& comps]
     (loop [comps (reverse comps)
            res   (num 0)]
       (if (not-empty comps)
         (recur
          (next comps)
          `(let [c# ~(first comps)]
             (if (== 0 c#)
               ~res
               c#)))
         res))))

(defn cmp [o1 o2]
  (if (nil? o1) 0
      (if (nil? o2) 0
          (compare o1 o2))))

(defn cmp-datoms-eavs [^Datom d1, ^Datom d2]
  (combine-cmp
   (#?(:clj Integer/compare :cljs -) (.-e d1) (.-e d2))
   (cmp (.-a d1) (.-a d2))
   (cmp (.-v d1) (.-v d2))
   (cmp (.-s d1) (.-s d2))))

(defn cmp-datoms-eav-only [^Datom d1, ^Datom d2]
  (combine-cmp
   (#?(:clj Integer/compare :cljs -) (.-e d1) (.-e d2))
   (cmp (.-a d1) (.-a d2))
   (cmp (.-v d1) (.-v d2))))

(defn- cmp-attr-quick [a1 a2]
  ;; either both are keywords or both are strings
  #?(:cljs
     (if (keyword? a1)
       (-compare a1 a2)
       (garray/defaultCompare a1 a2))
     :clj
     (.compareTo ^Comparable a1 a2)))

(defn cmp-datoms-eavs-quick [^Datom d1, ^Datom d2]
  (combine-cmp
   (#?(:clj Integer/compare :cljs -) (.-e d1) (.-e d2))
   (cmp-attr-quick (.-a d1) (.-a d2))
   (compare (.-v d1) (.-v d2))
   (cmp-attr-quick (.-s d1) (.-s d2))))

(defn empty-eavs []
  (set/sorted-set-by cmp-datoms-eavs))

(defn to-eavs [datoms]
  (if (seq datoms)
    (set/from-sequential cmp-datoms-eavs datoms)
    (empty-eavs)))

(defn empty-eav-only []
  (set/sorted-set-by cmp-datoms-eav-only))

(defn to-eav-only [datoms]
  (if (seq datoms)
    (->> datoms
         (reduce conj! (transient []))
         persistent!
         (set/from-sequential cmp-datoms-eav-only))
    (empty-eavs)))

(defn diff-in-value? [^Datom a ^Datom b]
  (and (= (.-e a) (.-e b))
       (= (.-a a) (.-a b))
       (not= (.-v a) (.-v b))))

(defn source-equals? [source ^Datom d]
  (identical? source (.s d)))

(defn contains-eav? [eavs ^Datom d]
  (set/slice eavs
             (datom (.-e d) (.-a d) (.-v d) nil)
             (datom (.-e d) (.-a d) (.-v d) nil)))
