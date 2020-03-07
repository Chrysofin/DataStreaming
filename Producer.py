"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    BROKER_URL = "PLAINTEXT://localhost:9092"
    CLIENT_ID = "station_producer"
    AVRO_PRODUCER = "PLAINTEXT://localhost:9092"#"http://localhost:8081"
    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = "com.udacity.project.transportation"
        self.key_schema = "arrival_key.key"
        self.value_schema = "arrival_value.value"
        self.num_partitions = 3
        self.num_replicas = 2

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": self.BROKER_URL,
            "client.id": "tranport_producer",
            "linger.ms": 1000,
            "compression.type": "lz4",
            "batch.num.messages": 100,
        }

        # If the topic does not already exist, try to create it
        print(topic_name)
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(self.AVRO_PRODUCER )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        logger.info(f"Creating topic: {self.topic_name}")
        adminclient = AdminClient({"bootstrap.servers": self.BROKER_URL})
        if self.topic_name not in Producer.existing_topics:        
            adminclient.create_topics(
                [
                    NewTopic(
                        topic =self.topic_name,
                        num_partitions= self.num_partitions,
                        replication_factor = 1,
                        config=
                        {
                            "cleanup.policy" :"compact",
                            "compression.type":"lz4",
                            "delete.retention.ms":100 ,  
                            "file.delete.delay.ms":100,
                            
                      }
                 )
             ]
         )
        #logger.info("topic creation kafka integration incomplete - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("producer closing...")
        self.close()
       

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

