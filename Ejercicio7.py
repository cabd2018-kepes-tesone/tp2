from pyspark import SparkConf, SparkContext
import sys, math

conf = SparkConf().setMaster("local").setAppName("Ejercicio7")
sc = SparkContext(conf = conf)

K = 5
centroides = sc.parallelize([(1, 10, 10), (2, 10, 80), (3, 80, 10), (4, 80, 80), (5, 50, 50)])

def armar_tupla(linea):
    tupla = linea.split('\t')
    return (int(tupla[0]), int(tupla[1]), int(tupla[2]), int(tupla[3]), tupla[4])

data_rdd = sc.textFile(sys.argv[1]) \
    .map(armar_tupla)

# error = 0.1
# dif = 1

for x in range(50):
    centroides = data_rdd.cartesian(centroides) \
        .map(lambda t: (t[0], (t[1][0], math.sqrt((t[0][1]-t[1][1])**2 + (t[0][2]-t[1][2])**2)))) \
        .reduceByKey(lambda t1, t2: t1 if t1[1] < t2[1] else t2) \
        .map(lambda t: (t[1][0], (1, t[0][1], t[0][2]))) \
        .reduceByKey(lambda t1, t2: (t1[0] + t2[0], t1[1] + t2[1], t1[2] + t2[2])) \
        .map(lambda t: (t[0], t[1][1]/t[1][0], t[1][2]/t[1][0]))

cantidadesVehiculos = data_rdd.cartesian(centroides) \
    .map(lambda t: (t[0], (t[1][0], math.sqrt((t[0][1]-t[1][1])**2 + (t[0][2]-t[1][2])**2)))) \
    .reduceByKey(lambda t1, t2: t1 if t1[1] < t2[1] else t2) \
    .map(lambda t: (t[1][0], None)) \
    .countByKey()

centroides.saveAsTextFile(sys.argv[2])
for key, value in cantidadesVehiculos.iteritems():
    print("en cluster "+str(key)+" corresponden "+str(value)+" vehiculos")

# calcular distancia
# quedarse con centroide con dist minima
# centroide como clave
# reubicar centroide: sumar lats y lons
# reubicar centroide: calcular promedio de lat y lon

# calcular distancia
# quedarse con centroide con dist minima
# centroide como clave
# contar para cada cluster cuantos vehiculos pasan

# (94 12 30 23423 Destino) (1 lat lon)
# (94 12 30 23423 Destino) (2 lat lon)
# (94 12 30 23423 Destino) (3 lat lon)

# (94 12 30 23423 Destino) (1 dist)
# (94 12 30 23423 Destino) (2 dist)
# (94 12 30 23423 Destino) (3 dist)

# (94 12 30 23423 Destino) (3 dist)

#
# 3 (cant, lat, lon)
# 3 (1, 12, 30)

# 3 (cantTotal, sumLat, sumLon)
