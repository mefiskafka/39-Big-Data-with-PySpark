'''
Logistic Regression model training
After creating labels and features for the data, we’re ready to build a model that can learn from it (training). But before you train the model, in this final part of the exercise, you'll split the data into training and test, run Logistic Regression model on the training data, and finally check the accuracy of the model trained on training data.

Remember, you have a SparkContext sc available in your workspace, as well as the samples variable.

Instructions
100 XP
Split the combined data into training and test datasets in 80:20 ratio.
Train the Logistic Regression model with the training dataset.
Create a prediction label from the trained model on the test dataset.
Combine the labels in the test dataset with the labels in the prediction dataset.
Calculate the accuracy of the trained model using original and predicted labels.
'''
# Split the data into training and testing
train_samples,test_samples = samples.randomSplit([0.8, 0.2])

# Train the model
model = LogisticRegressionWithLBFGS.train(train_samples)

# Create a prediction label from the test data
predictions = model.predict(test_samples.map(lambda x: x.features))

# Combine original labels with the predicted labels
labels_and_preds = test_samples.map(lambda x: x.label).zip(predictions)

# Check the accuracy of the model on the test data
accuracy = labels_and_preds.filter(lambda x: x[0] == x[1]).count() / float(test_samples.count())
print("Model accuracy : {:.2f}".format(accuracy))

