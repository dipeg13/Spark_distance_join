from pyspark.sql.functions import *
import math
from datetime import datetime


def boxes(dataframe, xy, boundaries, stop, key):
    global tree_dict
    global depth
    global partition
    if stop != depth:
        xmin = boundaries[0]
        xmax = boundaries[1]
        ymin = boundaries[2]
        ymax = boundaries[3]
        xy = 'x' if xy == 'y' else 'y'
        
        temp = dataframe.filter( (dataframe.x >=xmin) & (dataframe.x  <=xmax) & (dataframe.y >=ymin) & (dataframe.y  <= ymax))
        median = temp.stat.approxQuantile(xy, [.5], 0.1)[0]
        tree_dict[key] = median
        if xy == 'x':
            box1 = [xmin, median, ymin, ymax]
            box2 = [median, xmax, ymin, ymax]
        else:
            box1 = [xmin, xmax, ymin, median]
            box2 = [xmin, xmax, median, ymax]
        stop +=1
        keyLeft = key + '0'
        keyRight = key + '1'
        boxes(temp, xy, box1, stop, keyLeft)
        boxes(temp, xy, box2, stop, keyRight)
    else:
        tree_dict[key] = partition
        partition += 1 



tic = datetime.now()
epsilon = 0.1
path1 = "hdfs://node1:9000/user/user/un11000000"
path2 = "hdfs://node1:9000/user/user/un21000000"


tempA = spark.read.csv(path1, inferSchema=True).withColumnRenamed('_c0', 'x').withColumnRenamed('_c1', 'y')
tempB = spark.read.csv(path2, inferSchema=True).withColumnRenamed('_c0', 'x').withColumnRenamed('_c1', 'y')

varAx = tempA.agg({"x": "variance"}).collect()[0][0]
varAy = tempA.agg({"y": "variance"}).collect()[0][0]

varBx = tempB.agg({"x": "variance"}).collect()[0][0]
varBy = tempB.agg({"y": "variance"}).collect()[0][0]

Ahypo = varAx + varAy
Bhypo = varBx + varBy

if Ahypo < Bhypo:
    datasetA = tempA
    datasetB = tempB
else:
    datasetA = tempB
    datasetB = tempA


xmax = datasetA.agg({"x": "max"}).collect()[0][0]
ymax = datasetA.agg({"y": "max"}).collect()[0][0]
xmin = datasetA.agg({"x": "min"}).collect()[0][0]
ymin = datasetA.agg({"y": "min"}).collect()[0][0]
coord = [xmin, xmax, ymin, ymax]

from pyspark import StorageLevel
dfA = datasetA.sample(.05).persist(StorageLevel.MEMORY_ONLY)

import math

box_list = []
depth=9

nodes = 2**depth

tree_dict = dict()
key = '0'
partition = 0

boxes(dfA, "y", coord, 0, key)

@udf('int')
def cellID(x,y):
    global tree_dict
    global depth
    partFlag = '0'
    for i in range(depth):
        if len(partFlag)%2==1:
            if x <= tree_dict[partFlag]:
                partFlag += '0'
            else:
                partFlag += '1'
        else:
            if y <= tree_dict[partFlag]:
                partFlag += '0'
            else:
                partFlag += '1'
    return tree_dict[partFlag]

from pyspark.sql.types import * 
@udf(returnType=ArrayType(IntegerType()))
def cellIDB(x,y):
    global epsilon
    global tree_dict
    global depth
    partFlag = '0'
    idList = [partFlag]
    for i in range(depth):
        tempList = idList
        t = []
        #print(tempList)
        for check in tempList:
            print(check)
            if len(check)%2==1:
                if x + distk >= tree_dict[check]:
                    p = check+'1'
                    t.append(p)
                if x - distk <= tree_dict[check]:
                    p = check+'0'
                    t.append(p)
            else:
                if y + distk >= tree_dict[check]:
                    p = check+'1'
                    t.append(p)
                if y - distk <= tree_dict[check]:
                    p = check+'0'
                    t.append(p)
        idList = [j for j in t if len(j)==i+2]
    returnList = [tree_dict[i] for i in idList]
    return returnList

datasetB = datasetB.filter((datasetB.x >=xmin-epsilon) & (datasetB.x  <=xmax+epsilon) & (datasetB.y >=ymin-epsilon) & (datasetB.y  <= ymax+epsilon))


datasetADF = datasetA.withColumn('ID', cellID(col('x'),col('y')))
datasetBDF = datasetB.withColumn('ID', cellIDB(col('x'),col('y')))
datasetBDF = datasetBDF.select(datasetBDF.x, datasetBDF.y, explode(datasetBDF.ID)).withColumnRenamed('col', 'ID')

#proper partitioning based on pair-RDDs
"""
#datasetADF.groupBy('ID').count().show()
rddA = datasetADF.rdd.map(lambda x: (x[2], (x[0], x[1])))
rddB = datasetBDF.rdd.map(lambda x: (x[2], (x[0], x[1])))

def partitioner(key):
  return key

myRDDA = rddA.partitionBy(nodes, partitioner)
myRDDB = rddB.partitionBy(nodes, partitioner)

dataA = myRDDA.map(lambda x: (x[1][0], x[1][1], x[0])).toDF(["x", "y", "Box"])
dataB = myRDDB.map(lambda x: (x[1][0], x[1][1], x[0])).toDF(["x", "y", "Box"])
"""
dataA = datasetADF
dataB = datasetBDF

dataA = datasetADF.repartition(nodes, 'ID').persist(StorageLevel.MEMORY_ONLY)
dataB = datasetBDF.repartition(nodes, 'ID').persist(StorageLevel.MEMORY_ONLY)

dataA.createOrReplaceTempView("dataA")
dataB.createOrReplaceTempView("dataB")
toc = datetime.now()
preprocessingTime = (toc - tic).total_seconds()
print(preprocessingTime)


tic = datetime.now()
Joins = spark.sql('select a.x, a.y, b.x, b.y from dataA as a, dataB as b where a.ID=b.ID and power(a.x - b.x,2) + power(a.y - b.y,2) <= {0}'.format(epsilon*epsilon))

print("epsilon-distance Joins:",Joins.count())
toc = datetime.now()

queryTime = (toc - tic).total_seconds()
print(queryTime)
