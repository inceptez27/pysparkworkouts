from kafka import KafkaProducer
import time
#pip3 install kafka-python

def main():

    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    file = open("/home/hduser/hive/data/custs","r")
    filedata = file.readlines()
    for line in filedata:
        producer.send('my_topic', value=bytes(line,'utf-8'))
        producer.flush()
        print("Pushed")
        time.sleep(3)
        
        
main()