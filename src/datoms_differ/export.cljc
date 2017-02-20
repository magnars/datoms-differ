(ns datoms-differ.export
  (:require [datoms-differ.core :refer [get-datoms find-attrs]]
            [medley.core :refer [filter-keys map-vals]]))

(defn export [schema datoms]
  (str "#datascript/DB "
       (pr-str {:schema schema :datoms (vec datoms)})))

(defn prep-for-datascript
  "Filter away any keys from the schema contents that does not start with the :db/ namespace"
  [schema]
  (map-vals #(filter-keys (fn [k] (= (namespace k) "db")) %) schema))

(defn export-db [db]
  (export (prep-for-datascript (:schema db)) (get-datoms db)))

(defn prune-diffs [schema tx-data]
  (let [{:keys [many?]} (find-attrs schema)
        overwriting-additions (set (keep (fn [[op eid attr]]
                                           (when (and (= :db/add op)
                                                      (not (many? attr)))
                                             [eid attr]))
                                         tx-data))]
    (remove (fn [[op eid attr]]
              (and (= :db/retract op)
                   (overwriting-additions [eid attr])))
            tx-data)))
