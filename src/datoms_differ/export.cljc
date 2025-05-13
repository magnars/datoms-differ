(ns datoms-differ.export
  (:require [datoms-differ.core :refer [get-datoms]]
            [datoms-differ.impl.core-helpers :as ch]
            [medley.core :refer [filter-keys map-vals]]))

(def tx0 (inc (:to datoms-differ.core/default-db-id-partition)))

(defn export [schema datoms & {:keys [partition-key start-tx]
                               :or {partition-key :datoms-differ.core/db-id-partition
                                    start-tx tx0}}]
  (str "#datascript/DB "
       (pr-str {:schema (dissoc schema partition-key)
                :datoms (into [] (map #(conj % start-tx)) datoms)})))


(defn prep-for-datascript
  "Filter away any keys from the schema contents that does not start with the :db/ namespace"
  [schema]
  (map-vals #(filter-keys (fn [k] (= (namespace k) "db")) %) schema))

(defn ^:export export-db [db]
  (export (prep-for-datascript (:schema db)) (get-datoms db)))

(defn prune-diffs [schema tx-data]
  (let [{:keys [many?]} (ch/find-attrs schema)
        overwriting-additions (set (keep (fn [[op eid attr]]
                                           (when (and (= :db/add op)
                                                      (not (many? attr)))
                                             [eid attr]))
                                         tx-data))]
    (remove (fn [[op eid attr]]
              (and (= :db/retract op)
                   (overwriting-additions [eid attr])))
            tx-data)))

