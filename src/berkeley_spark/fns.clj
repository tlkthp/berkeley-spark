(ns berkeley-spark.fns
  (:require [sparkling.core :as spark])
  (:import (scala Tuple2)))

(defn less-than-10? [n] (< n 10))

(def gt-compr (comparator (fn [x y] (> x y))))

(defn singular-plural-pair [w] [w (str w "s")])

(defn tuple-vals-sum [^Tuple2 t]
  (let [k (._1 t)
        v (reduce + (._2 t))]
    (spark/tuple k v)))

(defn add [s] (reduce + s))

