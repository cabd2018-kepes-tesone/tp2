from pyspark import SparkConf, SparkContext
import sys, operator

conf = SparkConf().setMaster("local").setAppName("Ejercicio6")
sc = SparkContext(conf = conf)

umbral = 100

def armar_tupla(linea):
    tupla = linea.split('\t')
    return ((int(tupla[0]), int(tupla[1]), int(tupla[2])), int(tupla[3]))

data_rdd = sc.textFile(sys.argv[1]) \
    .map(armar_tupla)

# latitud
duplicado = data_rdd.map(lambda t: ((t[0][0], t[0][1], t[0][2]+1), t[1]))

# join ((id, lat, lon), (ts1, ts2))
latitudes = duplicado.join(data_rdd) \
    .filter(lambda t: abs(t[1][0] - t[1][1]) < umbral) \
    .map(lambda t: (t[0][1], None)) \
    .countByKey()

# longitud
duplicado = data_rdd.map(lambda t: ((t[0][0], t[0][1]+1, t[0][2]), t[1]))

# join ((id, lat, lon), (ts1, ts2))
longitudes = duplicado.join(data_rdd) \
    .filter(lambda t: abs(t[1][0] - t[1][1]) < umbral) \
    .map(lambda t: (t[0][2], None)) \
    .countByKey()

def max_key(d):
    return max(d.iteritems(), key=operator.itemgetter(1))[0]

print(max_key(latitudes), max_key(longitudes))
