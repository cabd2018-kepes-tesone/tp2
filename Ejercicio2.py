from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
import sys

conf = SparkConf().setMaster("local").setAppName("Ejercicio2")
sc = SparkContext(conf = conf)

sqlContext = SQLContext(sc)

data_rdd = sc.textFile(sys.argv[1])

data_sql = data_rdd.map(lambda l: l.split("\t")) \
    .map(lambda d: Row(id_vehiculo=int(d[0]), latitud=int(d[1]), longitud=int(d[2]), timestamp=int(d[3]), destino=d[4]))

data_df = sqlContext.createDataFrame(data_sql)

data_df.registerTempTable('trafico')

resultado = sqlContext.sql("SELECT destino, COUNT(*) FROM trafico WHERE destino <> '' AND destino <> 'Otro' GROUP BY destino ORDER BY COUNT(*) DESC LIMIT 3")

resultado.rdd.saveAsTextFile(sys.argv[2])

# rdd = sql.toRdd()
# for i = 1 to 3 do
#     result.union(rdd.get(i))

# result.saveAsText('Ejercicio2')
