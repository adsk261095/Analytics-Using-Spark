# Databricks notebook source
# Loading the marvel graph
marvel_graph = sc.textFile('/FileStore/tables/Marvel_graph-bca37.txt')

# COMMAND ----------

marvel_graph = marvel_graph.map(lambda x: x.split(' '))

# COMMAND ----------

# setting the source and target value as broadcast variables(immutable variables)
# in this way they will be shared among all the nodes in cluster
source = sc.broadcast('5306')
target = sc.broadcast('14')

# COMMAND ----------

# setting the counter value to 0 which will be
# shared among all the nodes in the cluster
counter = sc.accumulator(0)

# COMMAND ----------

# converting it  to rdd of tuple (node, connections)
marvel_connection = marvel_graph.map(lambda x: (x[0],x[1:-1]))

# COMMAND ----------

# total number of nodes in the graph
marvel_connection.count()

# COMMAND ----------

marvel_connection.take(1)

# COMMAND ----------

# combining the node having same key as one single node
marvel_connection = marvel_connection.reduceByKey(lambda x,y: x+y)

# COMMAND ----------

# total number of distinct nodes which have some connection
# is  equal to 6486
marvel_connection.count()

# COMMAND ----------

'''
this function basically creates rdd of tuple of form (nodeId, (connections, is_explored_or_not, distance))
is_explored_or_not basically has 3 vaues:
1. READY -> means this node is ready to be explored in subsequent iterations
2. NOT READY -> means this node is not ready to be explored as its immediate parents are not yet explored
3. DONE -> means this node has been completely explored and it should not be expllored again to prevent infinite loops

Distance -> means how much distance is the node away from the source.
Therefore, for source, distance = 0
and for other nodes we initialize the distance with large number(9999) initially
'''
def createNode(line):
  node = line[0]
  connection = line[1]
  explored = 'NOT READY'
  distance = 9999
  if node == source.value:
    distance = 0
    explored = 'READY'
  return (node, (connection, explored, distance))

# COMMAND ----------

marvel_nodes = marvel_connection.map(lambda x: createNode(x))

# COMMAND ----------

marvel_nodes.filter(lambda x:x[0]==source.value).collect()

# COMMAND ----------

counter.value = 0

# COMMAND ----------

'''
assigning the rdd marvel_node to bfstraversal rdd
just for better understanding
'''
bfsTraversal = marvel_nodes

# COMMAND ----------

'''
This is a mapper function which explores a node
and adds its adjacent connection into READY state
and updates their distance, it also changes state of
currecnt node to DONE after it is completely explored
'''
def checkForConnections(node):
  node_id = node[0]
  data = node[1]
  connections = data[0]
  explored = data[1]
  distance = data[2]
  result = list()
  if explored == 'READY':
    for connection in connections:
      if connection == target.value:
        counter.add(1)
      newNodeId = connection
      dist = distance + 1
      explored = 'READY'
      result.append((newNodeId,(list(), explored, dist)))
    result.append((node_id,(list(), 'DONE', distance)))
  else:
    result.append((node_id,(connections, explored, distance)))
  return result

# COMMAND ----------

'''
This reducer function reduces the bfsTraversal rdd
inorder to have latest status(READY, DONE or NOT READY)
and latest minimum from the source node
'''
def reducer(node1, node2):
  explored1 = node1[1]
  explored2 = node2[1]
  if explored1 == 'DONE' or explored2 == 'DONE':
    return (list(), 'DONE', min(node1[2], node2[2]))
  elif explored1 == 'READY' or explored2 == 'READY':
    return (node1[0] if len(node1[0])>len(node2[0]) else node2[0], 'READY', min(node1[2], node2[2]))
  else:
    pass

# COMMAND ----------

'''
Taking an uppper bound of 20 iteration
i.e the target would not be farther than
20 hops from the source node

After calling the mapper function we
check the value of acccumulator counter
if it is more than 0, it means that
target node has been found in iteration = i+1
and we break from the loop
'''
for i in range(20):
  bfsTraversal = bfsTraversal.flatMap(checkForConnections)
  print(bfsTraversal.count())
  bfsTraversal.collect()
  if counter.value > 0:
    print("target {} found at a distancce of {} from source {}".format(target.value, i+1, source.value))
    break
  bfsTraversal = bfsTraversal.reduceByKey(reducer)

# COMMAND ----------


