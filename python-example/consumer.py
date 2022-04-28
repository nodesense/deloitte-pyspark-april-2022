from kafka import KafkaConsumer

print("Consumer running.....")
# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('test',
                         group_id='test-group',
                         bootstrap_servers=['localhost:9092'])

# read messages from consumer
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("partition %d: office %d: key=%s value=%s" % ( message.partition,
                                          message.offset, message.key,
                                          message.value))