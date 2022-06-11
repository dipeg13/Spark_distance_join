from pyspark.sql.functions import *
import math
from datetime import datetime

path1 = "hdfs://node1:9000/user/user/blobs1.csv"
path2 = "hdfs://node1:9000/user/user/blobs2.csv"

tic = datetime.now()
datasetA = spark.read.csv(path1, inferSchema=True)
datasetB = spark.read.csv(path2, inferSchema=True)

datasetA = datasetA.withColumnRenamed('_c0', 'id').withColumnRenamed('_c1', 'x').withColumnRenamed('_c2', 'y').drop('id')
datasetB = datasetB.withColumnRenamed('_c0', 'id').withColumnRenamed('_c1', 'x').withColumnRenamed('_c2', 'y').drop('id')

xAmin = datasetA.agg({'x':'min'}).collect()[0][0]
xAmax = datasetA.agg({'x':'max'}).collect()[0][0]
yAmin = datasetA.agg({'y':'min'}).collect()[0][0]
yAmax = datasetA.agg({'y':'max'}).collect()[0][0]

xBmin = datasetB.agg({'x':'min'}).collect()[0][0]
xBmax = datasetB.agg({'x':'max'}).collect()[0][0]
yBmin = datasetB.agg({'y':'min'}).collect()[0][0]
yBmax = datasetB.agg({'y':'max'}).collect()[0][0]

xmin = xAmin if xAmin <= xBmin else xBmin
xmax = xAmax if xAmax >= xBmax else xBmax
ymin = yAmin if yAmin <= yBmin else yBmin
ymax = yAmax if yAmax >= yBmax else yBmax

n = 16
m = 16

distk = 0.1

@udf('int')
def cellIDA(x,y):
    global n
    global m
    global xmin
    global xmax
    global ymin
    global ymax
    x = (x - xmin)/(xmax - xmin)
    y = (y - ymin)/(ymax - ymin)
    x = math.floor(x * n)%n
    y = math.floor(y * m)%m
    return x + m * y
"""
from pyspark.sql.types import * 
@udf(returnType=ArrayType(IntegerType()))
def cellIDB(x,y):
    global n
    global m
    global xmin
    global xmax
    global ymin
    global ymax
    global distk
    dx = distk / (xmax - xmin)
    dy = distk / (ymax - ymin)
    Xlength = (xmax - xmin) / n
    Ylength = (ymax - ymin) / m
    Xs = []
    Ys = []
    x = (x - xmin)/(xmax - xmin)
    y = (y - ymin)/(ymax - ymin)
    x = math.floor(x * n)%n
    y = math.floor(y * m)%m
    Xs.append(x)
    Ys.append(y)
    while Xlength <= dx:
        


        dx -= distk
        
    return x + m * y
"""

datasetA = datasetA.withColumn('ID', cellIDA(col('x'), col('y')))
datasetB = datasetB.withColumn('ID', cellIDA(col('x'), col('y')))


dataA = datasetA.repartition(n*m, 'ID')
dataB = datasetB.repartition(n*m, 'ID')
toc = datetime.now()

print((toc-tic).total_seconds())
dataA.createOrReplaceTempView("dataA")
dataB.createOrReplaceTempView("dataB")
end_stamp = datetime.now()
temp = (end_stamp - start_stamp).total_seconds()
print(temp)

start_stamp = datetime.now()
Distance_joins = spark.sql('select a.x, a.y, b.x, b.y from dataA as a, dataB as b where a.ID=b.ID and power(a.x - b.x,2) + power(a.y - b.y,2) <= {0}'.format(distk * distk))

print("Total Joins: ",Distance_joins.count())
end_stamp = datetime.now()

algorithm = (end_stamp - start_stamp).total_seconds()
print(algorithm)





from pyspark.sql.functions import *
import math
from datetime import datetime

path1 = "hdfs://node1:9000/user/user/normal_1m_a.csv"
path2 = "hdfs://node1:9000/user/user/normal_1m_b.csv"

tic = datetime.now()
datasetA = spark.read.csv(path1, inferSchema=True)
datasetB = spark.read.csv(path2, inferSchema=True)

datasetA = datasetA.withColumnRenamed('_c0', 'id').withColumnRenamed('_c1', 'x').withColumnRenamed('_c2', 'y').drop('id')
datasetB = datasetB.withColumnRenamed('_c0', 'id').withColumnRenamed('_c1', 'x').withColumnRenamed('_c2', 'y').drop('id')

xAmin = datasetA.agg({'x':'min'}).collect()[0][0]
xAmax = datasetA.agg({'x':'max'}).collect()[0][0]
yAmin = datasetA.agg({'y':'min'}).collect()[0][0]
yAmax = datasetA.agg({'y':'max'}).collect()[0][0]

xBmin = datasetB.agg({'x':'min'}).collect()[0][0]
xBmax = datasetB.agg({'x':'max'}).collect()[0][0]
yBmin = datasetB.agg({'y':'min'}).collect()[0][0]
yBmax = datasetB.agg({'y':'max'}).collect()[0][0]

xmin = xAmin if xAmin <= xBmin else xBmin
xmax = xAmax if xAmax >= xBmax else xBmax
ymin = yAmin if yAmin <= yBmin else yBmin
ymax = yAmax if yAmax >= yBmax else yBmax

n = 16
m = 16

xLength = (xmax - xmin) / n
yLength = (ymax - ymin) / m

distk = 0.1

Xs = []
Ys = []

xzero = xmin
for i in range(n):
    Xs.append([xzero, xzero+xLength])
    xzero += xLength

yzero = ymin
for j in range(m):
    Ys.append([yzero, yzero+yLength])
    yzero += yLength

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


from pyspark.sql.types import * 
@udf(returnType=ArrayType(IntegerType()))
def cellIDB(x,y):
    global Xs
    global Ys
    global distk
    global m
    xID = []
    yID = []
    for i in range(len(Xs)):
        for j in range(len(Ys)):
            if x>= Xs[i][0]-distk and x<=Xs[i][1]+distk and y>=Ys[j][0]-distk and y<=Ys[j][1]+distk:
                xID.append(i)
                yID.append(j)
    ids = []
    for i in range(len(xID)):
        ids.append(xID[i]+yID[i]*m)
    return ids

dataA = datasetA.withColumn('ID', cellIDA(col('x'), col('y')))
dataB = datasetB.withColumn('ID', cellIDB(col('x'), col('y')))
dataB = dataB.select(dataB.x, dataB.y, explode(dataB.ID)).withColumnRenamed('col', 'ID')
"""
rddA = dataA.rdd.map(lambda x: (x[2], (x[0], x[1])))
rddB = dataB.rdd.map(lambda x: (x[2], (x[0], x[1])))

def partitioner(key):
  return key

myRDDA = rddA.partitionBy(n*m, partitioner)
myRDDB = rddB.partitionBy(n*m, partitioner)

dataA = myRDDA.map(lambda x: (x[1][0], x[1][1], x[0])).toDF(["x", "y", "ID"])
dataB = myRDDB.map(lambda x: (x[1][0], x[1][1], x[0])).toDF(["x", "y", "ID"])
"""
dataA = dataA.repartition(n*m, 'ID')
dataB = dataB.repartition(n*m, 'ID')

dataA.createOrReplaceTempView("dataA")
dataB.createOrReplaceTempView("dataB")

toc = datetime.now()
print((toc-tic).total_seconds())

start_stamp = datetime.now()
Distance_joins = spark.sql('select a.x, a.y, b.x, b.y from dataA as a, dataB as b where a.ID=b.ID and power(a.x - b.x,2) + power(a.y - b.y,2) <= {0}'.format(distk * distk))

print("Total Joins: ",Distance_joins.count())
end_stamp = datetime.now()

algorithm = (end_stamp - start_stamp).total_seconds()
print(algorithm)







    
