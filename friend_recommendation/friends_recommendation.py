# Databricks notebook source
friend_graph = sc.textFile('/FileStore/tables/soc_LiveJournal1Adj-2d179.txt')

# COMMAND ----------

# now we will create rdd of type (key, [connections])

# COMMAND ----------

def create_friends_rdd(x):
  x = x.split('\t')
  key = x[0]
  connections = x[1].split(',')
  return (key, connections)

# COMMAND ----------

friend_graph = friend_graph.map(create_friends_rdd)

# COMMAND ----------

'''
Now we will create MR1
map will create rdd of ((f1,f2),1 or 0)
reducer will reduce this rdd ((f1,f2),#no. of common friends)
'''

# COMMAND ----------

def map_pair_friend(x):
  result = list()
  for f1 in x[1]:
    '''
    adding pair person itself to friend
    with no. of friend in commmon to 0
    '''
    result.append(((x[0],f1), 0))
    for f2 in x[1]:
      if f1 != f2:
        # indicates that f1 and f2 are 2 diffferent people that have 1 friennd in common
        result.append(((f1,f2), 1))
  return result   

# COMMAND ----------

pair_rdd = friend_graph.flatMap(map_pair_friend)

# COMMAND ----------

pair_rdd.take(2)

# COMMAND ----------

'''
Now reducer will add of all common friends
between f1 and f2 only if f1 is not friend
of f2
'''

# COMMAND ----------

def reduce_common_friend_count(x,y):
  if x*y==0:
    return 0
  else:
    return x+y

# COMMAND ----------

common_friend_count = pair_rdd.reduceByKey(reduce_common_friend_count)

# COMMAND ----------

common_friend_count.take(2)

# COMMAND ----------

# filtering out the friends pair
common_friend_count = common_friend_count.filter(lambda x: x[1]!=0)

# COMMAND ----------

common_friend_count.cache()

# COMMAND ----------

'''
MR2
map - convert RDD of ((f1,f2), c1) to (f1, [(f2,c1)])
reduce - will reduce all the tuple with key as f1 
will give output like (f1 , [(f2,c1), (f3,c2)])
'''

# COMMAND ----------

def mapper2(x):
  return (x[0][0], [(x[0][1], x[1])])

# COMMAND ----------

friend_recom = common_friend_count.map(mapper2)

# COMMAND ----------

friend_recom.take(2)

# COMMAND ----------

'''
sorting the connection of each
in descending order of number of common friends
'''

# COMMAND ----------

friend_recom = friend_recom.sortBy(lambda x: -x[1][0][1])

# COMMAND ----------

friend_recom.take(2)

# COMMAND ----------

friend_recom.cache()

# COMMAND ----------

# reducer will combine value of all the keys

# COMMAND ----------

def combine_recommended_friends(x,y):
  return x+y

# COMMAND ----------

friend_recom = friend_recom.reduceByKey(combine_recommended_friends)

# COMMAND ----------

friend_recom.take(2)

# COMMAND ----------

'''
take_top_10 returns top 10
friend recommendations for each person
'''

# COMMAND ----------

def take_top_10(x):
  if len(x[1])<10:
    return (x[0],x[1])
  else:
    return (x[0],x[1][:10])

# COMMAND ----------

recommend = friend_recom.map(take_top_10)

# COMMAND ----------

recommend.take(2)

# COMMAND ----------


