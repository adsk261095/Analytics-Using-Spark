# Databricks notebook source
graph = sc.textFile('/FileStore/tables/web_Google-02a64.txt')

# COMMAND ----------

graph.take(5)

# COMMAND ----------

# removing the header rows from the dataset
graph = graph.filter(lambda x: '#' not in x)

# COMMAND ----------

graph.take(3)

# COMMAND ----------

def createGraph(x):
  result = list()
  result.append((x.split('\t')[0], [x.split('\t')[1]]))
  result.append((x.split('\t')[1], []))
  return result

# COMMAND ----------

# converting rdd of string into (key, value) pair rdd
graph = graph.flatMap(createGraph)

# COMMAND ----------

def reducingNode(x,y):
  return x+y

# COMMAND ----------

# doing a reduce by key operation get (key, [connections]) tuple
graph = graph.reduceByKey(reducingNode)

# COMMAND ----------

graph.take(3)

# COMMAND ----------

# storing the graph in the cache memory since it will be used again and again

# COMMAND ----------

graph.cache()

# COMMAND ----------

# total  number of node in the graph
graph.count()

# COMMAND ----------

# converting rdd to form (node, ([connection], initial_rank=1, rank got from other adjacent nodes))

# COMMAND ----------

graph = graph.map(lambda x: (x[0], (x[1], 1, 0)))

# COMMAND ----------

graph.cache()

# COMMAND ----------

def mapper(x):
  node_id = x[0]
  data = x[1]
  connections = data[0]
  rank = data[1] # would be 1 always
  transfer = data[2]
  result = list()
  '''
  rank of all the node that would come
  into this mapper function would be 1
  '''
  if len(connections) > 0:
    transfer = rank/len(connections)
    for connection in connections:
      result.append((connection, (list(), 0, transfer)))
    result.append((node_id, (list(), 0, 0)))
  else:
    result.append((node_id, (list(), 1, 0)))
  return result

# COMMAND ----------

ranked_graph = graph.flatMap(mapper)

# COMMAND ----------

'''
calling count(action) below because
transformation wouldn't be performed
until an action is called on it
'''

# COMMAND ----------

ranked_graph.count()

# COMMAND ----------

'''
reducer add up all the transfered 
rank from a given node
'''

# COMMAND ----------

def reducer(x,y):
  return (list(), 0, max(x[1], y[1]) + x[2] + y[2])

# COMMAND ----------

ranked_graph = ranked_graph.reduceByKey(reducer)

# COMMAND ----------

'''
after reducer is called total number of nodes
in graph should be equal to initial number of
nodes i.e 875713
'''

# COMMAND ----------

ranked_graph.count()

# COMMAND ----------

'''
verifying that total rank should be
equal to total number of nodes in the graph
i.e 875713
'''

# COMMAND ----------

rank = sc.accumulator(0)

# COMMAND ----------

'''
adding ranks of all the node
and adding the accumulator counter
with it to find the cumulative ranks
of the nodes in the graph
'''

# COMMAND ----------

def computeRank(x):
  rank.add(x[1][2])
  return True

# COMMAND ----------

a = ranked_graph.map(computeRank)

# COMMAND ----------

# calling count action so that map transformation is performed and counter is incremented

# COMMAND ----------

a.count()

# COMMAND ----------

# sum of rank of all the node is equal to total number of nodes 

# COMMAND ----------

rank.value

# COMMAND ----------

'''
Now we will be using damping factor (x*(0.85) + 0.15)
to find actual ranks 
of all the nodes in the graph
'''

# COMMAND ----------

rank_of_nodes = ranked_graph.map(lambda x: (x[0], x[1][2]*(0.85) + 0.15))

# COMMAND ----------

rank_of_nodes = rank_of_nodes.sortBy(lambda x: -x[1])

# COMMAND ----------

'''
Top 5 nodes with maximum rank
'''

# COMMAND ----------

rank_of_nodes.take(5)

# COMMAND ----------


