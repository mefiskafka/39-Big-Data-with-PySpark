'''
Comparing broadcast vs normal joins
You've created two types of joins, normal and broadcasted. Now your manager would like to know what the performance improvement is by using Spark optimizations. If the results are promising, you'll be given more opportunity to tweak the Spark setup as needed.

Your DataFrames normal_df and broadcast_df are available for your use.

Instructions
100 XP
Execute .count() on the normal DataFrame.
Execute .count() on the broadcasted DataFrame.
Print the count and duration of the DataFrames noting and differences.
'''

# Import the data to a DataFrame
departures_df = spark.read.csv('2015-departures.csv.gz', header=True)

# Remove any duration of 0
departures_df = departures_df.filter(departures_df[3] > 0)

# Add an ID column
departures_df = departures_df.withColumn('id', F.monotonically_increasing_id())

# Write the file out to JSON format
departures_df.write.json('output.json', mode='overwrite')
