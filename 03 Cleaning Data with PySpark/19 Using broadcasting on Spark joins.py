'''
Using broadcasting on Spark joins
Remember that table joins in Spark are split between the cluster workers. If the data is not local, various shuffle operations are required and can have a negative impact on performance. Instead, we're going to use Spark's broadcast operations to give each node a copy of the specified data.

A couple tips:

Broadcast the smaller DataFrame. The larger the DataFrame, the more time required to transfer to the worker nodes.
On small DataFrames, it may be better skip broadcasting and let Spark figure out any optimization on its own.
If you look at the query execution plan, a broadcastHashJoin indicates you've successfully configured broadcasting.
The DataFrames flights_df and airports_df are available to you.

Instructions
100 XP
Instructions
100 XP
Import the broadcast() method from pyspark.sql.functions.
Create a new DataFrame broadcast_df by joining flights_df with airports_df, using the broadcasting.
Show the query plan and consider differences from the original.
'''

start_time = time.time()
# Count the number of rows in the normal DataFrame
normal_count = normal_df.count()
normal_duration = time.time() - start_time

start_time = time.time()
# Count the number of rows in the broadcast DataFrame
broadcast_count = broadcast_df.count()
broadcast_duration = time.time() - start_time

# Print the counts and the duration of the tests
print("Normal count:\t\t%d\tduration: %f" % (normal_count, normal_duration))
print("Broadcast count:\t%d\tduration: %f" % (broadcast_count, broadcast_duration))