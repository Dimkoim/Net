import pandas as pd
from pyspark import SparkConf, SparkContext
from pairwise import *
from parseLine import *

#Initialize Spark
conf = SparkConf().setMaster("local").setAppName("netscalers")
sc = SparkContext(conf = conf)


#Csv file to RDD
lines = sc.textFile("data.csv")
#Parse and split a string and sortby the tracker_id and datetime column
rdd = lines.map(parseLine).sortBy(lambda a: (a[0], a[1]))
#RDD where key is the tracker_id and values the results of the pairwise operator on the type attribute
rdd = rdd.map(lambda x:(x[0], x[-1])).groupByKey().mapValues(pairwise)
#Flatten the tuple of tuples
rdd = rdd.map(lambda (x, y): y).flatMap(lambda xs: [subtup for subtup in xs])
#Count the occurence of every tuple
rdd = rdd.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
results = rdd.collect()


df = pd.DataFrame(results, columns = ['pair', 'count'])
df[['b1', 'b2']] = df['pair'].apply(pd.Series)
df_final = pd.concat([df['b1'], df['b2'], df['count']], axis = 1)
df_final['joined'] = df_final.b1.str.cat(df_final.b2)
#Checking for duplicates
if df_final.duplicated('joined').any()==True:
    print ('I found a duplicate here')
else:
    pass  
    
df_final.to_csv('results.csv', header= None, index = False)
print df_final
#for result in results:
#    print result

                
                            