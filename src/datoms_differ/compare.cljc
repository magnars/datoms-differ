;; Most code in this file is copied from DataScript (https://github.com/tonsky/datascript),
;; Copyright (c) 2014–2025 Nikita Prokopov.
;;
;; Original code licensed under the Eclipse Public License 1.0.
;; Modifications and integration into this project (c) 2025 Magnar Sveen.
;;
;; See the LICENSE file at the root of this project for the full EPL-1.0 text.

(ns datoms-differ.compare
  #?(:cljs (:require [goog.array :as garray]))
  #?(:cljs (:require-macros [datoms-differ.compare :refer [combine-cmp defn+ int-compare]])))

(defn- cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

#?(:clj
   (defmacro if-cljs
     "Return then if we are generating cljs code and else for Clojure code.
     https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
     [then else]
     (if (cljs-env? &env) then else)))

#?(:clj
   (defn patch-tag [meta cljs-env?]
     (if cljs-env?
       meta
       (condp = (:tag meta)
         'boolean (assoc meta :tag java.lang.Boolean)
         'number  (assoc meta :tag clojure.core$long)
         meta))))

#?(:clj
   (defmacro defn+
     "CLJS really don’t like :declared metadata on vars (generates less
      efficient code), but it needs it to skip warnings. So we redefine
      first with ^:declared and empty implementation, and then immediately
      redefine again without ^:declared. This way both `declare` and `defn+`
      versions have no ^:declared meta, thus allowing CLJS to generate direct
      invocations and see type hints."
     [name & rest]
     (let [name'    (vary-meta name patch-tag (cljs-env? &env))
           arglists (if (vector? (first rest))
                      [(first rest)]
                      (map #(list (first %)) rest))]
       (if (cljs-env? &env)
         `(do
            (defn ~(vary-meta name' assoc :declared true) ~@arglists)
            (defn ~name' ~@rest))
         `(defn ~name' ~@rest)))))

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

(defn cmp
  #?(:clj
     {:inline
      (fn [x y]
        `(let [x# ~x y# ~y]
           (if (nil? x#) 0 (if (nil? y#) 0 (long (compare x# y#))))))})
  ^long [x y]
  (if (nil? x) 0 (if (nil? y) 0 (long (compare x y)))))

(defn class-identical?
  #?(:clj  {:inline (fn [x y] `(identical? (class ~x) (class ~y)))})
  [x y]
  #?(:clj  (identical? (class x) (class y))
     :cljs (identical? (type x) (type y))))

#?(:clj
   (defn class-name
     {:inline
      (fn [x]
        `(let [^Object x# ~x]
           (if (nil? x#) x# (.getName (. x# (getClass))))))}
     ^String [^Object x] (if (nil? x) x (.getName (. x (getClass))))))

(defn class-compare
  ^long [x y]
  #?(:clj  (long (compare (class-name x) (class-name y)))
     :cljs (garray/defaultCompare (type->str (type x)) (type->str (type y)))))

#?(:clj
   (defmacro int-compare [x y]
     `(if-cljs
        (- ~x ~y)
        (long (Integer/compare ~x ~y)))))

(defn ihash
  {:inline (fn [x] `(. clojure.lang.Util (hasheq ~x)))}
  ^long [x]
  #?(:clj  (. clojure.lang.Util (hasheq x))
     :cljs (hash x)))

#?(:clj  (declare value-compare)
   :cljs (defn ^number value-compare [x y]))

(defn- seq-compare [xs ys]
  (let [cx (count xs)
        cy (count ys)]
    (cond
      (< cx cy)
      -1

      (> cx cy)
      1

      :else
      (loop [xs xs
             ys ys]
        (if (empty? xs)
          0
          (let [x (first xs)
                y (first ys)]
            (cond
              (and (nil? x) (nil? y))
              (recur (next xs) (next ys))

              (nil? x)
              -1

              (nil? y)
              1

              :else
              (let [v (value-compare x y)]
                (if (= v 0)
                  (recur (next xs) (next ys))
                  v)))))))))

(defn+ ^number value-compare [x y]
  (try
    (cond
      (= x y) 0
      (and (sequential? x) (sequential? y)) (seq-compare x y)
      #?@(:clj  [(instance? Number x)       (clojure.lang.Numbers/compare x y)])
      #?@(:clj  [(instance? Comparable x)   (.compareTo ^Comparable x y)]
          :cljs [(satisfies? IComparable x) (-compare x y)])
      (not (class-identical? x y)) (class-compare x y)
      #?@(:cljs [(or (number? x) (string? x) (array? x) (true? x) (false? x)) (garray/defaultCompare x y)])
      :else (int-compare (ihash x) (ihash y)))
    (catch #?(:clj ClassCastException :cljs js/Error) e
      (if (not (class-identical? x y))
        (class-compare x y)
        (throw e)))))

(defn value-cmp
  #?(:clj
     {:inline
      (fn [x y]
        `(let [x# ~x y# ~y]
           (if (nil? x#) 0 (if (nil? y#) 0 (value-compare x# y#)))))})
  ^long [x y]
  (if (nil? x)
    0
    (if (nil? y)
      0
      (value-compare x y))))
