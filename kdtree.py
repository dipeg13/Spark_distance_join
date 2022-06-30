#Ο κώδικας παρακάτω είνα πλήρως παραμετροποιήσιμος
#Μπορούν να οριστούν κάθε φορά τα paths για τα αρχεία στο HDFS, η απόσταση ε και το πλήθος των partitions
#Το πλήθος των partitions θα είναι 2^k όπου k το βάθος του δέντρου το οποίο δημιουργούμε
from pyspark.sql.functions import *
import math
from datetime import datetime

#Η μέθοδος nodesCreator υπολογίζει αναδρομικά τις διαμέσους και κωδικοποιεί όλη την απαραίτητη πληροφορία
#στο global λεξικό tree_dict, βάσει του βάθος του δέντρου το οποίο έχει a priori οριστεί
def nodesCreator(dataframe, xy, boundaries, stop, key):
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
            LD = [xmin, median, ymin, ymax]
            RU = [median, xmax, ymin, ymax]
        else:
            LD = [xmin, xmax, ymin, median]
            RU = [xmin, xmax, median, ymax]
        stop +=1
        keyLeft = key + '0'
        keyRight = key + '1'
        nodesCreator(temp, xy, LD, stop, keyLeft)
        nodesCreator(temp, xy, RU, stop, keyRight)
    else:
        tree_dict[key] = partition
        partition += 1 


#Εκκίνηση χρόνου υπολογισμού του preprocessing
tic = datetime.now()
#Ορισμός της απόστασης epsilon
epsilon = 0.001
#Ορισμός του path για το πρώτο αρχείο
#path1 = "hdfs://node1:9000/user/user/blobs11000000"
path1 = 'hdfs://node1:9000/user/user/blobs11000000'
#Oρισμός του path για το δεύτερο αρχείο
#path2 = "hdfs://node1:9000/user/user/blobs21000000"
path2 = 'hdfs://node1:9000/user/user/blobs21000000'

#Ορισμός των dataframes
tempA = spark.read.csv(path1, inferSchema=True).withColumnRenamed('_c0', 'x').withColumnRenamed('_c1', 'y')
tempB = spark.read.csv(path2, inferSchema=True).withColumnRenamed('_c0', 'x').withColumnRenamed('_c1', 'y')

#Υπολογισμός των διασπορών των ανά δύο διαφορετικών διαστάσεων
varAx = tempA.agg({'x': 'variance'}).collect()[0][0]
varAy = tempA.agg({'y': 'variance'}).collect()[0][0]

varBx = tempB.agg({'x': 'variance'}).collect()[0][0]
varBy = tempB.agg({'y': 'variance'}).collect()[0][0]

Ahypo = varAx + varAy
Bhypo = varBx + varBy

#Έλεγχος βέλτιστου dataset το οποίο θα χρησιμοποιηθεί για να δημιουργηθεί το tree_dict
if Ahypo < Bhypo:
    datasetA = tempA
    datasetB = tempB
else:
    datasetA = tempB
    datasetB = tempA

#Εύρεση των περιθωρίων του datasetA
xmax = datasetA.agg({"x": "max"}).collect()[0][0]
ymax = datasetA.agg({"y": "max"}).collect()[0][0]
xmin = datasetA.agg({"x": "min"}).collect()[0][0]
ymin = datasetA.agg({"y": "min"}).collect()[0][0]
coord = [xmin, xmax, ymin, ymax]

#Υποδειγματοληψία του datasetA και persist αυτού προκειμένου να χρησιμοποιηθεί για τον υπολογισμό του tree_dict
from pyspark import StorageLevel
dfA = datasetA.sample(.1).persist(StorageLevel.MEMORY_ONLY)

import math

#Ορισμός του βάθους του δέντρου depth, το οποίο ορίζει και το πλήθος των partitions ως #{partitions}=2^depth
depth=6

nodes = 2**depth

tree_dict = dict()
key = '0'
partition = 0

#Εύρεση των στοιχείων του tree_dict μέσω του υποδειγματολημένου συνόλου dfA απ' το datasetA
nodesCreator(dfA, 'y', coord, 0, key)

#Η udf για την αντιστοίχηση σημείων του Α στο αντίστοιχο partition
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

#H udf για την αντιστοίχηση σημείων του Β στα αντίστοιχα partitions (με τη μορφή λίστας)
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
                if x + epsilon >= tree_dict[check]:
                    p = check+'1'
                    t.append(p)
                if x - epsilon <= tree_dict[check]:
                    p = check+'0'
                    t.append(p)
            else:
                if y + epsilon >= tree_dict[check]:
                    p = check+'1'
                    t.append(p)
                if y - epsilon <= tree_dict[check]:
                    p = check+'0'
                    t.append(p)
        idList = [j for j in t if len(j)==i+2]
    returnList = [tree_dict[i] for i in idList]
    return returnList

#Περιορισμός του datasetB προκειμένου να γλιτώσουμε όσο το δυνατόν περισσότερο μπορούμε περιττούς υπολογισμούς
datasetB = datasetB.filter((datasetB.x >=xmin-epsilon) & (datasetB.x  <=xmax+epsilon) & (datasetB.y >=ymin-epsilon) & (datasetB.y  <= ymax+epsilon))

#Εφαρμογή των udfs και χρήση της explode για το datasetB
datasetADF = datasetA.withColumn('ID', cellID(col('x'),col('y')))
datasetBDF = datasetB.withColumn('ID', cellIDB(col('x'),col('y')))
datasetBDF = datasetBDF.select(datasetBDF.x, datasetBDF.y, explode(datasetBDF.ID)).withColumnRenamed('col', 'ID')

#Ορθό partitioning με χρήση pair-RDDs
rddA = datasetADF.rdd.map(lambda x: (x[2], (x[0], x[1])))
rddB = datasetBDF.rdd.map(lambda x: (x[2], (x[0], x[1])))

def partitioner(key):
  return key

myRDDA = rddA.partitionBy(nodes, partitioner)
myRDDB = rddB.partitionBy(nodes, partitioner)

dataA = myRDDA.map(lambda x: (x[1][0], x[1][1], x[0])).toDF(["x", "y", "ID"])
dataB = myRDDB.map(lambda x: (x[1][0], x[1][1], x[0])).toDF(["x", "y", "ID"])

"""
#partitioning το οποίο γίνεται με χρήση hash partitioner της Spark πάνω στις αντίστοιχες στήλες των dataframes
dataA = datasetADF
dataB = datasetBDF

dataA = datasetADF.repartition(nodes, 'ID')
dataB = datasetBDF.repartition(nodes, 'ID')
"""

dataA.createOrReplaceTempView("dataA")
dataB.createOrReplaceTempView("dataB")
toc = datetime.now()
#Τέλος μέτρησης χρόνου για το preproxessing
preprocessingTime = (toc - tic).total_seconds()
print(preprocessingTime)

#Έναρξη χρόνου μέτρησης για το query
tic = datetime.now()
Joins = spark.sql('select a.x, a.y, b.x, b.y from dataA as a, dataB as b where a.ID=b.ID and power(a.x - b.x,2) + power(a.y - b.y,2) <= {0}'.format(epsilon*epsilon))

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
