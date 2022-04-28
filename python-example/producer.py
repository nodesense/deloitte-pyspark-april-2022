from kafka import KafkaProducer
from kafka.errors import KafkaError

# broker runs in linux machine
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

TOPIC = "test"

# Asynchronous by default
# b' represent bytes
future = producer.send(TOPIC, b'welcome to kafka')
# after running this, look into linux consumer command subscribed for topic test

try:
    # get acknowledgement from broker
    record_metadata = future.get(timeout=10)
except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass

# Successful result returns assigned partition and offset
print ("topic", record_metadata.topic)
print ("partition", record_metadata.partition)
print ("offset",record_metadata.offset)

