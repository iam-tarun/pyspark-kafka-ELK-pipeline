import requests
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import time
import sys


# function to fetch the news headlines and send them to the given kafka topic
def fetchNews(url, topic):
  while 1:
    response = requests.get(url)
    for article in response.json()['articles']:
      # filter the news which are not received properly
      if article['content'] and article['content'] != '[Removed]':
        # send the content of each news to the given kafka topic
        topic_producer.send(topic, value=article['content'].encode('utf-8'))
        print("data sent.")
      topic_producer.flush()
    # wait for 300sec ( 5min ) and repeat the process
    time.sleep(300)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("""
        Usage: fetch_data.py <bootstrap-servers> <topic-to-store-news-data>
        """, file=sys.stderr)
        sys.exit(-1)

    # fetch the bootstrap servers, topic name from the command line arguments
    bootstrap_servers = sys.argv[1]
    topic = sys.argv[2]
    print(bootstrap_servers)
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    # check if the given topic is present in kafka, if not create one.
    topic_metadata = admin_client.list_topics()
    if topic in topic_metadata:
        print(f"Topic '{topic}' exists.")
    else:
        print(f"Topic '{topic}' does not exist.")
        print(f"Creating '{topic}.'")
        admin_client.create_topics([NewTopic(name=topic, num_partitions=3, replication_factor=1)])

    # Close Kafka admin client
    admin_client.close()


    topic_producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    url = ('https://newsapi.org/v2/top-headlines?'
          'country=us&'
          'apiKey=2c7c29a92e8446f69c1d6c02c16c98f7')
    
    fetchNews(url, topic)