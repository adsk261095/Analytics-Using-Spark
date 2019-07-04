# Databricks notebook source
customer = sc.textFile('/FileStore/tables/customer.txt')
orders = sc.textFile('/FileStore/tables/orders.txt')

# COMMAND ----------

customer.count()

# COMMAND ----------

orders.count()

# COMMAND ----------

# analyzing the content of both the datasets

# COMMAND ----------

customer.take(3)

# COMMAND ----------

orders.take(3)

# COMMAND ----------

# Question 1 - Top 5 states with most of cancelled orders

# COMMAND ----------

#fetch out (customer_id, state) tuple from customer rdd
customer = customer.map(lambda x:(x.split(',')[0], x.split(',')[-2]))

# COMMAND ----------

customer.top(2)

# COMMAND ----------

# filter canceled orders out of orders dataset

# COMMAND ----------

canceled_orders = orders.filter(lambda x: x.split(',')[-1]=='CANCELED')

# COMMAND ----------

# keep only (customer_id,order_id) from canceled orders rdd
canceled_orders = canceled_orders.map(lambda x: (x.split(',')[-2], x.split(',')[0]))

# COMMAND ----------

# (customer_id,order_id) from canceled orders rdd
canceled_orders.top(3)

# COMMAND ----------

# do join of canceled orders with that of customer datset

# COMMAND ----------

cust_order = customer.join(canceled_orders)

# COMMAND ----------

cust_order.top(3)

# COMMAND ----------

# using map form rdd having tuple of type(state,1)

# COMMAND ----------

state_count = cust_order.map(lambda x: (x[1][0], 1))

# COMMAND ----------

state_count.top(3)

# COMMAND ----------

# do a reduceByKey operation while adding the values of the same keys

# COMMAND ----------

state_wise_canceled_count = state_count.reduceByKey(lambda x,y: x+y)

# COMMAND ----------

state_wise_canceled_count.count()

# COMMAND ----------

#sort the rdd of tuples in descending order of values

# COMMAND ----------

state_wise_canceled_count = state_wise_canceled_count.sortBy(lambda x: -x[1])

# COMMAND ----------

state_wise_canceled_count.collect()

# COMMAND ----------

state_wise_canceled_count.take(5)

# COMMAND ----------


