# mlSpark
Apache Spark application as end to end use case from data acquisition, transformation, model training and deployment.

## Procedure
1. [setting.sh](/scripts/setting.sh): Pull a Dataset of Accelerometer data from [here](https://github.com/wchill/HMP_Dataset).
2. [input.py](/scripts/input.py): Load dataset and store it as a dataframe.
3. [transfrom.py](/scripts/transform.py): Transform the data into parquet files.
4. [train.py](/scripts/train.py): Train and get the model file using pyspark ml.
5. [deploy.py](/scripts/deploy.py): Deploy model into watson IBM Cloud.


---
### Errors 
> Exception: Java gateway process exited before sending its port number

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