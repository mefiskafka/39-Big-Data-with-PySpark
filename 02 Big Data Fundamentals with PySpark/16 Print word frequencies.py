'''
Print word frequencies
After combining the values (counts) with the same key (word), in this exercise, you'll return the first 10 word frequencies. You could have retrieved all the elements at once using collect() but it is bad practice and not recommended. RDDs can be huge: you may run out of memory and crash your computer..

What if we want to return the top 10 words? For this, first you'll need to swap the key (word) and values (counts) so that keys is count and value is the word. After you swap the key and value in the tuple, you'll sort the pair RDD based on the key (count). This way it is easy to sort the RDD based on the key rather than the key using sortByKey operation in PySpark. Finally, you'll return the top 10 words from the sorted RDD.

You already have a SparkContext sc and resultRDD available in your workspace.

Instructions
Print the first 10 words and their frequencies from the resultRDD RDD.
Swap the keys and values in the resultRDD.
Sort the keys according to descending order.
Print the top 10 most frequent words and their frequencies from the sorted RDD.
'''


# Display the first 10 words and their frequencies from the input RDD
for word in resultRDD.take(10):
	print(word)

# Swap the keys and values from the input RDD
resultRDD_swap = resultRDD.map(lambda x: (x[1], x[0]))

# Sort the keys in descending order
resultRDD_swap_sort = resultRDD_swap.sortByKey(ascending=False)

# Show the top 10 most frequent words and their frequencies from the sorted RDD
for word in resultRDD_swap_sort.take(10):
	print("{},{}". format(word[1], word[0]))