# Import Library
from pyspark.context import SparkContext
from awsglue.context import GlueContext  
glueContext = GlueContext(SparkContext.getOrCreate())  
from pyspark.sql.functions import concat, lit, upper, regexp_replace

#Get source in Athena
database = "default"
table = "csv"

# Table Input
customer = glueContext.create_dynamic_frame_from_catalog(database=database, table_name=table)
gender= glueContext.create_dynamic_frame_from_catalog(database=database, table_name="gender")

#Create the Data Frame
customer_df = customer.toDF()

# Upper to columns with data type is string
customer_df = customer_df.withColumn("nomecliente",upper(customer_df["nomecliente"]))
customer_df = customer_df.withColumn("endereco",upper(customer_df["endereco"]))
customer_df = customer_df.withColumn("complemento",upper(customer_df["complemento"]))
customer_df = customer_df.withColumn("cidade",upper(customer_df["cidade"]))
customer_df = customer_df.withColumn("estado",upper(customer_df["estado"]))
customer_df = customer_df.withColumn("pais",upper(customer_df["pais"]))

# Drop the two last columns fo data frame
customer_df = customer_df.drop("numeroempregadovendedor")   

#replace value of the column
customer_df =customer_df.withColumn('sexo', regexp_replace('sexo', 'M', 'MASCULINO'))
customer_df =customer_df.withColumn('sexo', regexp_replace('sexo', 'F', 'FEMININO'))

#Rename name EUA for ESTADOS UNIDOS
customer_df =customer_df.withColumn('pais', regexp_replace('pais', 'EUA','ESTADOS UNIDOS'))

#Rename values is null.
customer_df = customer_df.filter(customer_df.cep =='').withColumn('cep', regexp_replace('cep', '', '-1'))
customer_df = customer_df.filter(customer_df.cep =='').withColumn('estado', regexp_replace('cep', '', 'NAO INFORMADO'))

#Select with the concat the two columns in one column
customer_df = customer_df.select("numerocliente",
                   concat(upper(customer_df['primeironomecontato']), lit(' '), upper(customer_df['ultimonomecontato'])).alias('Contato'),     
				   "sexo",
				   "telefone",
				   "pais",
				   "estado",
				   "cidade",
				   "endereco",
				   "complemento",
				   "cep"
                    )

# Write the file in S3
customer_df.write.mode('overwrite').format('parquet').save('s3://athena-aws-tests-s3/dimensions/customer/',header= 'false')