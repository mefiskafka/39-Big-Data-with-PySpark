'''
Create the modeler
The Estimator you'll be using is a LogisticRegression from the pyspark.ml.classification submodule.

Import the LogisticRegression class from pyspark.ml.classification.
Create a LogisticRegression called lr by calling LogisticRegression() with no arguments.
'''

# Import LogisticRegression
from pyspark.ml.classification import LogisticRegression

# Create a LogisticRegression Estimator
lr = LogisticRegression()