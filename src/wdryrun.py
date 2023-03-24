import pyspark
import pyspark.sql.types as T
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.functions import concat,concat_ws

#postgressql into spark

spark = SparkSession.builder.config("spark.jars","postgresql-42.5.3.jar").master("local").appName("PySpark_Postgres_test").getOrCreate()
df = spark.read.format("jdbc").option("url","jdbc:postgresql://ec2-13-40-49-105.eu-west-2.compute.amazonaws.com:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "wanekaterraluna").option("user", "consultants").option("password","WelcomeItc@2022").load()

print(df.printSchema())

#Pyspark Transformation

sortdf = df.sort(col("price").desc())
sortdf.show()

# Create Hive table
sortdf.write.mode('overwrite') \
    .saveAsTable("pythongroup.wanekaterraluna")