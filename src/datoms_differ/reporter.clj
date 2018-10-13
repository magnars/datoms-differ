(ns datoms-differ.reporter
  (:require [clansi]
            [clojure.set :as set]
            [clojure.string :as str]
            [datoms-differ.core :as core]))

(defn replace-entity-ids-with-identifier [schema datoms]
  (let [{:keys [ref? identity?]} (core/find-attrs schema)
        reverse-id-lookup (into {} (keep (fn [[e a v]]
                                           (when (identity? a)
                                             [e [a v]])) datoms))]
    (for [[e a v] datoms]
      [(reverse-id-lookup e)
       a
       (if (ref? a)
         (reverse-id-lookup v)
         v)])))

(defn find-individual-changes [old-datoms new-datoms]
  (let [old-datoms (set old-datoms)
        new-datoms (set new-datoms)
        removed (set/difference (set old-datoms) (set new-datoms))
        added (set/difference (set new-datoms) (set old-datoms))
        changed (->> (concat removed added)
                     (group-by (fn [[e a _]] [e a]))
                     (vals)
                     (filter next))
        removed (set/difference removed (set (map first changed)))
        added (set/difference added (set (map second changed)))]
    (-> []
        (into (for [datom removed]
                (into [:removed] datom)))
        (into (for [datom added]
                (into [:added] datom)))
        (into (for [[[e a old-v] [_ _ new-v]] changed]
                [:changed e a [old-v new-v]])))))

(defn max-by [k coll]
  (when (seq coll)
    (apply max-key k coll)))

(defn group-changes [changes]
  (loop [result []
         remaining-changes changes]
    (if (empty? remaining-changes)
      result
      (let [same-e (->> remaining-changes
                        (group-by (fn [[event e a v]] e))
                        (max-by (fn [[_ entries]] (count entries))))
            same-event+a (->> remaining-changes
                              (group-by (fn [[event e a v]] [event a]))
                              (max-by (fn [[_ entries]] (count entries))))
            num-same-e-entries (count (second same-e))
            num-same-event+a-entries (count (second same-event+a))]
        (cond
          (= 1 num-same-e-entries num-same-event+a-entries)
          (into result remaining-changes)

          (> num-same-event+a-entries num-same-e-entries)
          (recur (conj result
                       (let [[[event a] entries] same-event+a]
                         [:several-entities event a (for [[_ e _ v] entries] [e v])]))
                 (remove (set (second same-event+a)) remaining-changes))

          :else
          (recur (conj result
                       (let [[e entries] same-e]
                         [:same-entity e (for [[event _ a v] entries] [event a v])]))
                 (remove (set (second same-e)) remaining-changes)))))))

(defn- summarize-entity [e datoms]
  (into {} (keep (fn [[e* a v]]
                   (when (and (= e* e) (not= e [a v]))
                     [a v]))
                 datoms)))

(defn- fffirst [mmm]
  (first (ffirst mmm)))

(defn find-entire-entity-changes [old-datoms new-datoms]
  (let [old-es (set (map first old-datoms))
        new-es (set (map first new-datoms))
        removed-es (set/difference old-es new-es)
        added-es (set/difference new-es old-es)
        removed-entities (for [e removed-es] [e (summarize-entity e old-datoms)])
        added-entities (for [e added-es] [e (summarize-entity e new-datoms)])
        changed-es (let [removed-vals->es (group-by second removed-entities)]
                     (keep (fn [[e added-vals]]
                             (when-let [es (removed-vals->es added-vals)]
                               (when-not (next es)
                                 [(ffirst es) e])))
                           added-entities))
        changed? (set (mapcat identity changed-es))
        removed-entities (remove (fn [[e summary]] (changed? e)) removed-entities)
        added-entities (remove (fn [[e summary]] (changed? e)) added-entities)]
    (cond-> []
      (seq removed-entities) (into (for [[k v] (group-by ffirst removed-entities)]
                                     [:removed-entities k v]))
      (seq added-entities) (into (for [[k v] (group-by ffirst added-entities)]
                                   [:added-entities k v]))
      (seq changed-es) (conj [:changed-identities changed-es]))))

(defn find-changes [old-datoms new-datoms]
  (let [entire-entity-changes (find-entire-entity-changes old-datoms new-datoms)
        handled-old-entity-id? (set (mapcat (fn [[k t v]]
                                              (cond
                                                (= :removed-entities k) (map first v)
                                                (= :changed-identities k) (map first t)
                                                :else nil))
                                            entire-entity-changes))
        handled-new-entity-id? (set (mapcat (fn [[k t v]]
                                              (cond
                                                (= :added-entities k) (map first v)
                                                (= :changed-identities k) (map second t)
                                                :else nil))
                                            entire-entity-changes))]
    (concat
     entire-entity-changes
     (group-changes (find-individual-changes
                     (remove #(handled-old-entity-id? (first %)) old-datoms)
                     (remove #(handled-new-entity-id? (first %)) new-datoms))))))

(defn- pr-data [data]
  (let [s (pr-str data)]
    (if (< 38 (bounded-count 39 s))
      (str (subs s 0 35) "..." (last s))
      s)))

(defn summarize-entities [entities direction verb]
  (let [groups (group-by (juxt ffirst second) entities)]
    (if (< 4 (bounded-count 5 groups)) ;; too many, summarize instead of iterating
      (for [[e entities] (group-by ffirst entities)]
        (str direction " " (count entities) "× [" e "], e.g. "
             (let [[[_ id] v] (first entities)]
               (str verb " " (pr-data v) " for " (pr-str id)))))
      (for [[[e v] entities] groups]
        (let [num (count entities)]
          (str verb " " (pr-data v) " for " num "× [" e "]"
               (if (< num 4) ": " ", e.g. ")
               (str/join " " (take 3 (map (comp pr-str second first) entities)))))))))

(defn create-report [changes]
  (for [[t & args] changes]
    (cond
      (= :changed t) (let [[e a [old-v new-v]] args]
                       {:text (str "Changed " e " " a " to " (pr-data new-v))
                        :details [(str "was " (pr-data old-v))]})
      (= :removed t) (let [[e a v] args]
                       {:text (str "Removed " a " from " e)
                        :details [(str "was " (pr-data v))]})
      (= :added t) (let [[e a v] args]
                     {:text (str "Added " a " " (pr-data v) " to " e)})

      (= :changed-identities t) (let [[changes] args]
                                  {:text (str (count changes) " entities changed identity")
                                   :details (for [[before after] changes]
                                              (str before " to " after))})

      (#{:added-entities :removed-entities} t)
      (let [verb (if (= :added-entities t) "Added" "Removed")
            [type entities] args]
        (if (next entities)
          (let [num (count entities)]
            {:text (str verb " " num "× [" type "] entities")
             :details (cond->
                          (vec (take 3 (for [[[_ id] vals] entities]
                                         (str (pr-data id) " " (pr-data vals)))))
                        (< 3 num) (conj (str "... and " (- num 3) " more.")))})
          {:text (str verb " entity " (ffirst entities))
           :details (for [[k v] (second (first entities))]
                      (str (pr-data k) " " (pr-data v)))}))

      (= :same-entity t)
      (let [[e changes] args]
        {:text (str "Changed 2 attributes for " e)
         :details (for [[event a v] changes]
                    (cond
                      (= :added event) (str "added " a " " (pr-data v))
                      (= :removed event) (str "removed " a " " (pr-data v))
                      (= :changed event) (str "changed " a " to " (pr-data (second v)))))})

      (= :several-entities t)
      (let [[t a entities] args]
        (cond
          (= :removed t)
          {:text (str "Removed " a " from " (count entities) " entities")
           :details (summarize-entities entities "from" "was")}

          (= :added t)
          {:text (str "Added " a " to " (count entities) " entities")
           :details (summarize-entities entities "to" "is")}

          (= :changed t)
          {:text (str "Changed " a " for " (count entities) " entities")
           :details (summarize-entities entities "for" "replaced")})))))

(defn render-report! [report]
  (doseq [{:keys [text details]} report]
    (println (clansi/style
              text
              (cond
                (str/starts-with? text "Added ") :green
                (str/starts-with? text "Removed ") :red
                :else :cyan)))
    (doseq [detail details]
      (println "-" detail))
    (println " ")))
