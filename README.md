# mlSpark
Apache Spark application as end to end use case from data acquisition, transformation, model training and deployment.

## Procedure
1. Extract the data from [Public Dataset of Accelerometer Data for Human Motion Primitives Detection](https://github.com/wchill/HMP_Dataset) and store it as a dataframe.
2. Transform the data into parquet files.
3. Train and get the model using pyspark ml.
4. Deploy model into watson IBM Cloud.


---
### Errors 
#### Exception: Java gateway process exited before sending its port number

1. Make sure you have JAVA8 (macOS)

```Shell
brew tap adoptopenjdk/openjdk
brew install --cask adoptopenjdk8
```

2. Find your JAVA8's home directory then add those two lines.

```python
import os
os.environ['JAVA_HOME'] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
```