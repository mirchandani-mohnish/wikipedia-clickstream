from kafka import KafkaConsumer

topic = 'hello-world'
consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092', auto_offset_reset='earliest')

for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
