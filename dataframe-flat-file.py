from pyspark.sql import SparkSession
spark = SparkSession.builder \
  .appName('trendy-tech-training')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()


spark.conf.set("spark.sql.repl.eagerEval.enabled",True)
spark.conf.set("spark.scheduler.mode", "FAIR")
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')


df_input = spark.read.text("gs://spark_training/word_count_trendy_tech.txt")

rdd = df_input.rdd


counts = rdd.flatMap(lambda x: x[0].split(" ")) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b)
                            
#def func_Split(x):
    #return x.split(" ")
    
#print(rdd.collect())
print(counts.collect())
#print(rdd.__class__)
