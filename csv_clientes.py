# Importando os context Spark e Glue Context
from pyspark.context import SparkContext
from awsglue.context import GlueContext  
glueContext = GlueContext(SparkContext.getOrCreate())   

#Lendo a tabela csv do banco de dados 'Default' que foi gerada pelo Crawler que le o arquivo no S3 e grava na tabela.
csv = glueContext.create_dynamic_frame_from_catalog(database="default", table_name="csv")

df = csv.toDF
df.show()
#Spark SQL
csv_upper = spark.sql("Select replace(UPPER(NomeCliente),',','.') Cliente, upper(Pais) Pais, upper(estado) Estado from csv")     

#df.createOrReplaceTempView("V_Customer") criando uma view

# spark.sql("Select * from V_Customer")  lendo a view

                    
#Gerando o arquivo de saida no S3 com base no select da linha anterior
csv_upper.write.mode('overwrite').format('csv').save('s3://athena-aws-tests-s3/csv_upper/',header= 'false')   

