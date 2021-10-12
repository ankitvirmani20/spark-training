from pyspark.sql import SparkSession
spark = SparkSession.builder \
  .appName('trendy-tech-training')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()


spark.conf.set("spark.sql.repl.eagerEval.enabled",True)
spark.conf.set("spark.scheduler.mode", "FAIR")
spark.conf.set("google.cloud.auth.service.account.enable", "true")


rdd_input=spark.sparkContext.textFile("gs://spark_training/word_count_trendy_tech.txt")

counts = rdd_input.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)


print(rdd_input.__class__)

for element in counts.collect():
    print(element)
