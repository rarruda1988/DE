# Mount the Doogle Drive for have access the your files.
from google.colab import drive
drive.mount('/content/drive')

#Install Pyspark and Pandas.
!pip install pyspark
!pip install pandas
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
        
 #create the var with the path of csv file.      
path = "/content/drive/MyDrive/Colab Notebooks/TSLA.csv"

#DF with Pandas.
pd.read_csv(path)

DF with Spark.
df = spark.read.csv(path, header= True)
df.show()


