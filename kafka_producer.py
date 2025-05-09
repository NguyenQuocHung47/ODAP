from kafka import KafkaProducer
import csv
import time
import random

def send_to_kafka(): 
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    with open('credit_card_transactions-ibm_v2.csv', 'r') as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader):
            #row = {key: (value if value.strip() else "nan") for key, value in row.items()}
            message = str(row).encode('utf-8')
            producer.send('transactions', message)
            print(f"Sending message: {message.decode('utf-8')}")
            time.sleep(random.uniform(1, 3))  
    producer.close()

send_to_kafka() 
