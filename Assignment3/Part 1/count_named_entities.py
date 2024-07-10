from pyspark.sql import SparkSession
import nltk
from pyspark.sql.functions import explode, udf, struct, to_json
from pyspark.sql.types import ArrayType, StringType
import spacy
from kafka.admin import KafkaAdminClient, NewTopic
import sys





# find the named entities from the given sentence
def fetchNamedEntities(line: str):
  doc = nlp(line.lower())
  named_entities = [ent.text for ent in doc.ents]
  return named_entities

# clean the data
def clean_text(tokens):
  tokens = [lemmatizer.lemmatize(token) for token in tokens if len(token) > 4 and token not in eng_stopwords and token.isalpha()]
  return tokens


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("""
        Usage: count_named_entities.py <bootstrap-servers> <topic1> <topic2> <spark-jar-packages> <checkpoint-location>
        """, file=sys.stderr)
        sys.exit(-1)

    # fetch the bootstrap servers, topic name from the command line arguments
    bootstrap_servers = sys.argv[1]
    topic1 = sys.argv[2]
    topic2 = sys.argv[3]
    sparkJarPackages = sys.argv[4]
    checkpoint = sys.argv[5]
    print(bootstrap_servers)
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    # check if the given topic is present in kafka, if not create one.
    topic_metadata = admin_client.list_topics()
    if topic2 in topic_metadata:
        print(f"Topic '{topic2}' exists.")
    else:
        print(f"Topic '{topic2}' does not exist.")
        print(f"Creating '{topic2}.'")
        admin_client.create_topics([NewTopic(name=topic2, num_partitions=3, replication_factor=1)])
    
    nlp = spacy.load("en_core_web_sm")

    lemmatizer = nltk.WordNetLemmatizer()
    eng_stopwords = nltk.corpus.stopwords.words('english')

    spark = SparkSession.builder.appName("CountNamedEntities").config("spark.jars.packages", sparkJarPackages).getOrCreate() #"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

    spark.sparkContext.setLogLevel("ERROR")

    df = spark\
            .readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", bootstrap_servers)\
            .option("subscribe", topic1)\
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false")\
            .load()

    test = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")



    fetchNamedEntities_udf = udf(fetchNamedEntities, ArrayType(StringType()))
    clean_text_udf = udf(clean_text, ArrayType(StringType()))

    test = test.withColumn("key", fetchNamedEntities_udf(test['value']))
    test = test.withColumn("key", clean_text_udf(test["key"]))
    test = test.withColumn("key", explode(test["key"])).groupBy('key').count()

    # combining both key and count columns into single json value under "value" column
    test = test.withColumn("value", to_json(struct("key", "count")))

    query =  test\
      .writeStream\
      .outputMode('complete') \
      .format("kafka") \
      .option("kafka.bootstrap.servers", bootstrap_servers) \
      .option("topic", topic2) \
      .option("checkpointLocation", checkpoint) \
      .option("offsets", "latest")\
      .start()

    query.awaitTermination()

