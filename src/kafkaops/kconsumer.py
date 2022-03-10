from kafka import KafkaConsumer

def main():
    consumer = KafkaConsumer('my_topic',bootstrap_servers='localhost:9092')
    for message in consumer:
        data = str(message.value)
        print (data)
        

main()