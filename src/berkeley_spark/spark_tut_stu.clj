(ns berkeley-spark.spark-tut-stu
  (:require
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de]
            [berkeley-spark.fns :as fn]))

;;;;;;;;;;;;;
;;
;; Part 2: An introduction to using Apache Spark with Sparkling running in Clojure REPL
;;
;;;;;;;;;;;;;


;; Creating Spark Context
;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-spark-context [app-name]
  (let [c (-> (conf/spark-conf)
              (conf/master "local[*]")
              (conf/app-name app-name))]
    (spark/spark-context c)))

(def sc (make-spark-context "Spark Tutorial Student"))

; Display the type of the Spark Context sc
(type sc)
;=> org.apache.spark.api.java.JavaSparkContext

;; (2b) SparkContext attributes

;; (2c) Getting help

;;;;;;;;;;;;;
;;
;; Part 3: Using RDDs and chaining together transformations and actions
;;
;;;;;;;;;;;;;

;; Working with your first RDD
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; (3a) Create a Clojure seq of integers in the range of 1 .. 10000
(def data (range 1 1001))

; Obtain data's first element
(first data)
;=> 1

; We can check the size of the list using the count function
(count data)
;=> 1000

;; (3b) Distributed data and using a collection to create an RDD
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Parallelize data using 8 partitions
; This operation is a transformation of data into an RDD
; Spark uses lazy evaluation, so no Spark jobs are run at this point
(def x-range-rdd (spark/parallelize sc 8 data))

; Let's see what type sc.parallelize() returned
(type x-range-rdd)
;=> org.apache.spark.api.java.JavaRDD

; Each RDD gets a unique ID
; https://spark.apache.org/docs/latest/api/java/index.html
(.id x-range-rdd)
;=> 33

; We can name each newly created RDD using the setName() method
(spark/rdd-name "My First RDD" x-range-rdd)
;=> #<JavaRDD My First RDD ParallelCollectionRDD[33] at parallelize at NativeMethodAccessorImpl.java:-2>

; Let's view the lineage (the set of transformations) of the RDD using toDebugString()
(.toDebugString x-range-rdd)
;=> "(8) My First RDD ParallelCollectionRDD[33] at parallelize at NativeMethodAccessorImpl.java:-2 []"

; Let's see how many partitions the RDD will be split into by using the getNumPartitions()
(spark/count-partitions x-range-rdd)
;=> 8

;; (3c): Subtract one from each value using map
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Transform xrangeRDD through map transformation using 'dec' function
; Because map is a transformation and Spark uses lazy evaluation, no jobs, stages,
; or tasks will be launched when we run this code.
(def sub-rdd (spark/map dec x-range-rdd))

; Let's see the RDD transformation hierarchy
(.toDebugString sub-rdd)
;=> "(8) MapPartitionsRDD[34] at map at NativeMethodAccessorImpl.java:-2 []\n
; |  My First RDD ParallelCollectionRDD[0] at parallelize at NativeMethodAccessorImpl.java:-2 []"


;; (3d) Perform action collect to view results
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Let's collect the data
(spark/collect sub-rdd)
;=> [0 1 2 3 4 5 6 7 8 9 ... 999]


;; (3d) Perform action count to view counts
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(spark/count sub-rdd)
;=> 1000

;; (3e) Apply transformation filter and view results with collect
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Define a function to filter a single value -> berkelyx-cs100-1x.fns/less-than-10?

(comment
  ; ***** NOTE *****
  ; The following example will only work in an AOT-compiled environment.
  ; So, it will not work in your REPL. Refer below Part 4 section.
  (spark/filter (fn [n] (< n 10)) sub-rdd)
  )

(def filtered-rdd (spark/filter fn/less-than-10? sub-rdd))
(spark/collect filtered-rdd)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Part 4: Lambda Functions
;;
;; Refer to 'Passing Functions to sparkling' section at Sparkling getting started guide:
;; https://gorillalabs.github.io/sparkling/articles/getting_started.html#rdd-operations
;;
;; If you want to quickly try Spark on REPL then do the followings:
;; 1. define your fns in separate namespace (e.g. berkelyx-cs100-1x.fns),
;; 2. add your namespace in :aot vector in project.clj file,
;; 3. restart your REPL, now you can use your new fn with Spark.
;;
;; that's what I did with 'less-than-10?' fn used above and other fns used in this tutorial.
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;
;;
;; Part 5: Additional RDD actions
;;
;;;;;;;;;;;;;

;; (5a) Other common actions
;;  first(), take(), top(), takeOrdered(), and reduce()
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Let's get the first element
(spark/first filtered-rdd)

; the first 4
(spark/take 4 filtered-rdd)

; Note that it is ok to take more elements than the RDD has
(spark/take 12 filtered-rdd)

; Retrieve the three smallest elements
; Sparkling does not provide wrapper fn for 'takeOrdered' so we have to use Java version
(.takeOrdered filtered-rdd 3)

; Retrieve the five largest elements
; Sparkling does not provide wrapper fn for 'top' so we have to use Java version
(.top filtered-rdd 5)

; Pass a lambda function to takeOrdered to reverse the order
; filteredRDD.takeOrdered(4, lambda s: -s)
(.takeOrdered filtered-rdd 3 fn/gt-compr)

; # Efficiently sum the RDD using reduce
(spark/reduce + filtered-rdd)
;=> 45

; Note that subtraction is not both associative and commutative
(spark/reduce - filtered-rdd)
;=> -45
(spark/reduce - (spark/repartition 4 filtered-rdd))
;=> 9

; While addition is
(spark/reduce + (spark/repartition 4 filtered-rdd))
;=> 45


;; (5b) Advanced actions
;;;;;;;;;;;;;;;;;;;;;;;;

;; Here are two additional actions that are useful for retrieving information from an RDD:
;; takeSample() and countByValue().

; takeSample reusing elements
(.takeSample filtered-rdd true 6)
;=> [5 2 9 9 1 1] ; will get different results on different runs

; takeSample without reuse
(.takeSample filtered-rdd false 6)
;=> [9 4 7 2 0 3] ; will get different results on different runs

; Set seed for predictability
(.takeSample filtered-rdd true 6 500)
;=> [4 5 9 7 6 3] ; gives same result for multiple runs

; Create new base RDD to show countByValue
(def repetitive-rdd (spark/parallelize sc [1 2 3 1 2 3 1 2 1 2 3 3 3 4 5 4 6]))
(spark/count-by-value repetitive-rdd)
;=> {5 1, 1 4, 6 1, 2 4, 3 5, 4 2}


;;;;;;;;;;;;;
;;
;; Part 6: Additional RDD transformations
;;
;;;;;;;;;;;;;

;; (6a) flatMap
;;;;;;;;;;;;;;;

; Let's create a new base RDD to work from
(def words-list ["cat" "elephant" "rat" "rat" "cat"])
(def words-rdd (spark/parallelize sc words-list))

; Use map
(def singular-n-plural-words-rdd-map (spark/map fn/singular-plural-pair words-rdd))
(spark/collect singular-n-plural-words-rdd-map)
;=> [["cat" "cats"] ["elephant" "elephants"] ["rat" "rats"] ["rat" "rats"] ["cat" "cats"]]

; Use flatMap
(def singular-n-plural-words-rdd (spark/flat-map fn/singular-plural-pair words-rdd))
(spark/collect singular-n-plural-words-rdd)
;=> ["cat" "cats" "elephant" "elephants" "rat" "rats" "rat" "rats" "cat" "cats"]

; View the number of elements in the RDD
(spark/count singular-n-plural-words-rdd-map)
;=> 5

(spark/count singular-n-plural-words-rdd)
;=> 10

;; (6b) groupByKey and reduceByKey
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def pair-rdd
  (spark/parallelize-pairs sc [(spark/tuple "a" 1)
                               (spark/tuple "a" 2)
                               (spark/tuple "b" 1)]))
; Different ways to sum by key:
;1
(->> pair-rdd
     spark/group-by-key
     (spark/map fn/tuple-vals-sum)
     spark/collect)
;=> [#sparkling/tuple ["a" 3] #sparkling/tuple ["b" 1]]

;2
; Using mapValues, which is recommended when they key doesn't change
(->> pair-rdd
     spark/group-by-key
     (spark/map-values fn/add)
     spark/collect)
;=> [#sparkling/tuple ["a" 3] #sparkling/tuple ["b" 1]]

;3
; reduceByKey is more efficient / scalable
(->> pair-rdd
     (spark/reduce-by-key +)
     spark/collect)
;=> [#sparkling/tuple ["a" 3] #sparkling/tuple ["b" 1]]


;; (6c) Advanced transformations [Optional]
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;  mapPartitions() and mapPartitionsWithIndex()
;; TODO

;;;;;;;;;;;;;
;;
;; Part 7: Caching RDDs and storage options
;;
;;;;;;;;;;;;;

;; (7a) Caching RDDs
;;;;;;;;;;;;;;;;;;;;

; Name the RDD
(spark/rdd-name "My Filtered RDD" filtered-rdd)
; Cache the RDD
(spark/cache filtered-rdd)
; Is it cached
; It seems that there is no Java API to check if RDD is cached.

;; (7b) Unpersist and storage options
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; Note that toDebugString also provides storage information
(.toDebugString filtered-rdd)
;=> "(8) My Filtered RDD MapPartitionsRDD[3] at filter at NativeMethodAccessorImpl.java:-2 [Memory Deserialized 1x Replicated]\n
; |  MapPartitionsRDD[2] at map at NativeMethodAccessorImpl.java:-2 [Memory Deserialized 1x Replicated]\n
; |  My First RDD ParallelCollectionRDD[0] at parallelize at NativeMethodAccessorImpl.java:-2 [Memory Deserialized 1x Replicated]"

; If we are done with the RDD we can unpersist it so that its memory can be reclaimed
(.unpersist filtered-rdd)
;=> #<JavaRDD My Filtered RDD MapPartitionsRDD[3] at filter at NativeMethodAccessorImpl.java:-2>

; Storage level for a non cached RDD
(.getStorageLevel filtered-rdd)
;=> #<StorageLevel StorageLevel(false, false, false, false, 1)>

; Storage level for a cached RDD
(spark/cache filtered-rdd)
(.getStorageLevel filtered-rdd)
;=> #<StorageLevel StorageLevel(false, true, false, true, 1)>

;;;;;;;;;;;;;
;;
;; Part 8: Debugging Spark applications and lazy evaluation
;;
;;;;;;;;;;;;;