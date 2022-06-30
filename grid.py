#Ο κώδικας παρακάτω είνα πλήρως παραμετροποιήσιμος
#Μπορούν να οριστούν κάθε φορά τα paths για τα αρχεία στο HDFS, η απόσταση ε και το πλήθος των partitions
#Το πλήθος των partitions θα είναι n*m, όπου τα n και m μπορούμε να τα ορίσουμε a priori
from pyspark.sql.functions import *
import math
from datetime import datetime

#Εκκίνηση χρόνου υπολογισμού του preprocessing
tic = datetime.now()
#Ορισμός του path για το πρώτο αρχείο
path1 = "hdfs://node1:9000/user/user/blobs11000000"
#Ορισμός του path για το δεύτερο αρχείο
path2 = "hdfs://node1:9000/user/user/blobs21000000"

#Ορισμός των dataframes
datasetA = spark.read.csv(path1, inferSchema=True).withColumnRenamed('_c0', 'x').withColumnRenamed('_c1', 'y')
datasetB = spark.read.csv(path2, inferSchema=True).withColumnRenamed('_c0', 'x').withColumnRenamed('_c1', 'y')

#Υπολογισμός των συνόρων και των δύο datasets
xAmin = datasetA.agg({'x':'min'}).collect()[0][0]
xAmax = datasetA.agg({'x':'max'}).collect()[0][0]
yAmin = datasetA.agg({'y':'min'}).collect()[0][0]
yAmax = datasetA.agg({'y':'max'}).collect()[0][0]

xBmin = datasetB.agg({'x':'min'}).collect()[0][0]
xBmax = datasetB.agg({'x':'max'}).collect()[0][0]
yBmin = datasetB.agg({'y':'min'}).collect()[0][0]
yBmax = datasetB.agg({'y':'max'}).collect()[0][0]

#Υπολογισμός των ολικών συνόρων
xmin = xAmin if xAmin <= xBmin else xBmin
xmax = xAmax if xAmax >= xBmax else xBmax
ymin = yAmin if yAmin <= yBmin else yBmin
ymax = yAmax if yAmax >= yBmax else yBmax

#Ορισμός του πλήθους των γραμμών n
n = 16
#Ορισμός του πλήθους των στηλών m
m = 16

xLength = (xmax - xmin) / n
yLength = (ymax - ymin) / m

#Ορισμός της απόστασης epsilon
epsilon = 0.001

Xs = []
Ys = []

#Υπολογισμός των κοψιμάτων στον x άξονα
xzero = xmin
for i in range(n):
    Xs.append([xzero, xzero+xLength])
    xzero += xLength

#Υπολογισμός των κοψιμάτων στον y άξονα
yzero = ymin
for j in range(m):
    Ys.append([yzero, yzero+yLength])
    yzero += yLength

#Η udf για την αντιστοίχηση σημείων του Α στο αντίστοιχο partition
@udf('int')
def cellIDA(x,y):
    global Xs
    global Ys
    xID = -1
    yID = -1
    for i in range(len(Xs)):
        for j in range(len(Ys)):
            if x>= Xs[i][0] and x<=Xs[i][1] and y>=Ys[j][0] and y<=Ys[j][1]:
                xID = i
                yID = j
    return xID + len(Ys)*yID

#H udf για την αντιστοίχηση σημείων του Β στα αντίστοιχα partitions (με τη μορφή λίστας)
from pyspark.sql.types import * 
@udf(returnType=ArrayType(IntegerType()))
def cellIDB(x,y):
    global Xs
    global Ys
    global epsilon
    global m
    xID = []
    yID = []
    for i in range(len(Xs)):
        for j in range(len(Ys)):
            if x>= Xs[i][0]-epsilon and x<=Xs[i][1]+epsilon and y>=Ys[j][0]-epsilon and y<=Ys[j][1]+epsilon:
                xID.append(i)
                yID.append(j)
    ids = []
    for i in range(len(xID)):
        ids.append(xID[i]+yID[i]*m)
    return ids

#Εφαρμογή των udfs και χρήση της explode για το datasetB
dataA = datasetA.withColumn('ID', cellIDA(col('x'), col('y')))
dataB = datasetB.withColumn('ID', cellIDB(col('x'), col('y')))
dataB = dataB.select(dataB.x, dataB.y, explode(dataB.ID)).withColumnRenamed('col', 'ID')

#Ορθό partitioning με χρήση pair-RDDs
rddA = dataA.rdd.map(lambda x: (x[2], (x[0], x[1])))
rddB = dataB.rdd.map(lambda x: (x[2], (x[0], x[1])))

def partitioner(key):
  return key

myRDDA = rddA.partitionBy(n*m, partitioner)
myRDDB = rddB.partitionBy(n*m, partitioner)

dataA = myRDDA.map(lambda x: (x[1][0], x[1][1], x[0])).toDF(["x", "y", "ID"])
dataB = myRDDB.map(lambda x: (x[1][0], x[1][1], x[0])).toDF(["x", "y", "ID"])

"""
#partitioning το οποίο γίνεται με χρήση hash partitioner της Spark πάνω στις αντίστοιχες στήλες των dataframes
dataA = dataA.repartition(n*m, 'ID')
dataB = dataB.repartition(n*m, 'ID')

dataA.createOrReplaceTempView("dataA")
dataB.createOrReplaceTempView("dataB")
"""
toc = datetime.now()
#Τέλος μέτρησης χρόνου για το preproxessing
preprocessingTime = (toc-tic).total_seconds()
print(preprocessingTime)

#Έναρξη χρόνου μέτρησης για το query
tic = datetime.now()
Joins = spark.sql('select a.x, a.y, b.x, b.y from dataA as a, dataB as b where a.ID=b.ID and power(a.x - b.x,2) + power(a.y - b.y,2) <= {0}'.format(epsilon * epsilon))

print("epsilon-distance Joins:",Joins.count())
toc = datetime.now()
#Τέλος μέτρησης χρόνου εκτέλεσης του query
#Εδώ γίνεται χρήση του action .count() 

queryTime = (toc - tic).total_seconds()
print(queryTime)

#Επιλογή για αποθήκευση των αποτελεσμάτων του query
saveResults = False
if saveResults:
    Joins.write.csv('Results.csv')
