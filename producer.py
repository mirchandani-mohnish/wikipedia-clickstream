from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'hello-world'

for i in range(1, 10):
    message = f"Hello World {i}"
    producer.send(topic, value=message.encode('utf-8'))
    print(f"Sent: {message}")
    time.sleep(1)

producer.close()
