# Import Library
from pyspark.context import SparkContext
from awsglue.context import GlueContext  
glueContext = GlueContext(SparkContext.getOrCreate())  
from pyspark.sql.functions import concat, lit, upper, regexp_replace

#Get source in Athena
database = "default"
table = "pedidos"

# Table Input
pedidos = glueContext.create_dynamic_frame_from_catalog(database=database, table_name=table)

df = pedidos.toDF()

#Drop Column Comentarios
df = df.drop("comentario")   
df = df.drop("numerocliente")   

#Upper the column situacao
df = df.withColumn("situacao",upper(df["situacao"]))

# Write the file in S3
customer_df.write.mode('overwrite').format('parquet').save('s3://athena-aws-tests-s3/dimensions/dim_pedidos/',header= 'false')