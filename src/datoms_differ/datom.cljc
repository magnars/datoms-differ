(ns datoms-differ.datom
  (:require [me.tonsky.persistent-sorted-set :as sset])
  (:import (clojure.lang Seqable IPersistentCollection Indexed ILookup Associative MapEntry)
           (java.lang IllegalArgumentException Object UnsupportedOperationException)))

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

  Seqable
  (seq [d] (seq-datom d))

  IPersistentCollection
  (equiv [d o] (and (instance? Datom o) (equiv-datom d o)))
  (empty [_] (throw (UnsupportedOperationException. "empty is not supported on Datom")))
  (count [_] 4)
  (cons [d [k v]] (assoc-datom d k v))

  Indexed
  (nth [this i]           (nth-datom this i))
  (nth [this i not-found] (nth-datom this i not-found))

  ILookup
  (valAt [d k] (val-at-datom d k nil))
  (valAt [d k nf] (val-at-datom d k nf))

  Associative
  (entryAt [d k] (when-let [v (val-at-datom d k nil)]
                   (MapEntry. k v)))
  (containsKey [_ k] (#{:e :a :v :s} k))
  (assoc [d k v] (assoc-datom d k v)))


(defn datom ^Datom [e a v s] (Datom. e a v s 0))


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
    :e (.-e d)
    :a (.-a d)
    :v (.-v d)
    :s (.-s d)
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

(defn- assoc-datom ^Datom [^Datom d k v]
  (case k
    :e (datom v       (.-a d) (.-v d) (.-s d) )
    :a (datom (.-e d) v       (.-v d) (.-s d) )
    :v (datom (.-e d) (.-a d) v       (.-s d) )
    :s (datom (.-e d) (.-a d) (.-v d) v            )
    (throw (IllegalArgumentException. (str "invalid key for Datom: " k)))))

#?(:clj
   (defmethod print-method Datom [^Datom d, ^java.io.Writer w]
     (.write w (str "#differ/Datom "))
     (binding [*out* w]
       (pr [(.-e d) (.-a d) (.-v d) (.-s d)]))))

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

(defprotocol DatomValueComparator
  "A protocol that allows you to provide custom compare for types of your choosing for the value attribute of a datom"
  (compare-value [this other]))

(defn cmp [o1 o2]
  (if (nil? o1) 0
      (if (nil? o2) 0
          (compare o1 o2))))

(extend-protocol DatomValueComparator
  Object
  (compare-value [o1 o2] (cmp o1 o2))

  nil
  (compare-value [_ _] 0))

(defn cmp-datoms-eavs [^Datom d1, ^Datom d2]
  (combine-cmp
   (#?(:clj Integer/compare :cljs -) (.-e d1) (.-e d2))
   (cmp (.-a d1) (.-a d2))
   (compare-value (.-v d1) (.-v d2))
   (cmp (.-s d1) (.-s d2))))

(defn cmp-datoms-eav-only [^Datom d1, ^Datom d2]
  (combine-cmp
   (#?(:clj Integer/compare :cljs -) (.-e d1) (.-e d2))
   (cmp (.-a d1) (.-a d2))
   (compare-value (.-v d1) (.-v d2))))

(defn empty-eavs []
  (sset/sorted-set-by cmp-datoms-eavs))

(defn to-eavs [datoms]
  (if (seq datoms)
    (sset/from-sequential cmp-datoms-eavs datoms)
    (empty-eavs)))

(defn to-eav-only [datoms]
  (if (seq datoms)
    (->> datoms
         (reduce conj! (transient []))
         persistent!
         (sset/from-sequential cmp-datoms-eav-only))
    (empty-eavs)))

(defn diff-in-value? [^Datom a ^Datom b]
  (and (= (.-e a) (.-e b))
       (= (.-a a) (.-a b))
       (not= (.-v a) (.-v b))))

(defn source-equals? [source ^Datom d]
  (= source (.s d)))

(defn contains-eav? [eavs ^Datom d]
  (sset/slice eavs
              (datom (.-e d) (.-a d) (.-v d) nil)
              (datom (.-e d) (.-a d) (.-v d) nil)))
