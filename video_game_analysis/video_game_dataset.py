# Databricks notebook source
vd = sc.textFile('/FileStore/tables/vgsales_cleaned.csv')

# COMMAND ----------

vd.top(1)

# COMMAND ----------

vd.count()

# COMMAND ----------

vd = vd.filter(lambda x: 'Rank' not in x)

# COMMAND ----------

# Removed header row from the dataset
vd.count()

# COMMAND ----------

# getting platforms from whole dataset
p = vd.map(lambda x: x.split(',')).map(lambda x: x[2])

# COMMAND ----------

# counting number of distinct of platforms
p.distinct().count()

# COMMAND ----------

# total global for each platform

# COMMAND ----------

#we have to create RDD of (key, value) pair i.e (platform, global sales)

# COMMAND ----------

#ps is RDD of  (platform, golbal sales)
ps = vd.map(lambda x: x.split(',')).map(lambda x: (x[2],float(x[-1])))

# COMMAND ----------

ps.take(3)

# COMMAND ----------

reduced_ps = ps.reduceByKey(lambda x,y: x+y)

# COMMAND ----------

reduced_ps = reduced_ps.sortBy(lambda x: -x[1])

# COMMAND ----------

reduced_ps.distinct().count()

# COMMAND ----------

reduced_ps.count()

# COMMAND ----------

reduced_ps.collect()

# COMMAND ----------

#top 10 platforms by global sales

# COMMAND ----------

reduced_ps.take(10)

# COMMAND ----------

#count number of games of each platform type

# COMMAND ----------

pg = ps.map(lambda x: (x[0],1))

# COMMAND ----------

#aggerate the count for each platform type

# COMMAND ----------

pg = pg.reduceByKey(lambda x,y: x+y)

# COMMAND ----------

pg.count()

# COMMAND ----------

#using a negative sign in order to sort in descending order
pg = pg.sortBy(lambda x: -x[1])

# COMMAND ----------

pg.collect()

# COMMAND ----------

#maximum value of each platform global sales

# COMMAND ----------

#method 1 is to reduceByKey while keeping track of max value till now for that particular key

# COMMAND ----------

# ps is (platformm, global sales) is a (key,value) pair RDD
# pms is (platformm, max global sales) is a (key,value) pair RDD
pms = ps.reduceByKey(lambda x,y : max(x,y))

# COMMAND ----------

pms = pms.sortBy(lambda x: -x[1])

# COMMAND ----------

pms.collect()

# COMMAND ----------

# method 2 is to groupByKey and find max value out of list of values

# COMMAND ----------

# ps is (platformm, global sales) is a (key,value) pair RDD
# pgs is (platformm, list of global sales) is a (key,value) pair RDD
pgs = ps.groupByKey()

# COMMAND ----------

pgs = pgs.map(lambda x: (x[0], max(list(x[1]))))

# COMMAND ----------

pgs = pgs.sortBy(lambda x: -x[1])

# COMMAND ----------

pgs.collect()

# COMMAND ----------


