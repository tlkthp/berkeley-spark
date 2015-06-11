(defproject berkeley-spark "0.1.0-SNAPSHOT"
            :description "FIXME: write description"
            :url "http://example.com/FIXME"
            :license {:name "Eclipse Public License"
                      :url  "http://www.eclipse.org/legal/epl-v10.html"}
            :dependencies [[org.clojure/clojure "1.6.0"]
                           [gorillalabs/sparkling "1.2.1-SNAPSHOT"]]

            :aot [#".*" sparkling.serialization sparkling.destructuring]

            :repl-options {:init-ns berkeley-spark.spark-tut-stu}

            :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.10 "1.3.1"]]}
                       :dev      {:plugins [[lein-dotenv "RELEASE"]]}})
