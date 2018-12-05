from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
import sys

conf = SparkConf().setMaster("local").setAppName("Ejercicio5")
sc = SparkContext(conf = conf)

sqlContext = SQLContext(sc)

data_rdd = sc.textFile(sys.argv[1])

franja = int(sys.argv[3])

data_sql = data_rdd.map(lambda l: l.split("\t")) \
    .map(lambda d: Row(grupo=int(d[3])/franja, id_vehiculo=int(d[0])))

data_df = sqlContext.createDataFrame(data_sql)

data_df.registerTempTable('trafico')

consulta = "SELECT grupo*%d AS limite_inferior, grupo*%d+%d AS limite_superior, COUNT(DISTINCT id_vehiculo) FROM trafico GROUP BY grupo ORDER BY grupo" % (franja, franja, franja)

resultado = sqlContext.sql(consulta)

resultado.rdd.saveAsTextFile(sys.argv[2])
