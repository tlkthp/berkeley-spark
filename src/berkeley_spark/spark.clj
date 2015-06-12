(ns berkeley-spark.spark
  (:require [sparkling.core :as spark]))

(defn map-into-tuples [m]
  (map (fn [[k v]] (spark/tuple k v)) m))

(defn parallelize-map
  [m]
  (let [lst (map-into-tuples m)]
    (spark/parallelize-pairs m)))

(defn take-ordered
  ([n rdd]
   (.takeOrdered rdd n))
  ([n compr rdd]
   (.takeOrdered rdd n compr)))

(defn top
  ([n rdd] (.top rdd n))
  ([n compr rdd] (.top rdd n compr)))

(defn take-sample
  ([with-replacement n rdd] (.takeSample rdd with-replacement n))
  ([with-replacement n seed rdd] (.takeSample rdd with-replacement n seed)))

(defn storage-level
  [rdd] (.getStorageLevel rdd))

(defn unpersist
  ([rdd] (.unpersist rdd))
  ([blocking? rdd] (.unpersist rdd blocking?)))
