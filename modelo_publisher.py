from pika import *
import json
import pika
import os
from dotenv import load_dotenv

load_dotenv('app/config/.env')

class RabbitmqPublisher:
    def __init__(self):
        self.credentials = pika.PlainCredentials(username=os.getenv('user_mq'), password=os.getenv('senha_mq'))
        self.parameters = pika.ConnectionParameters(host=os.getenv('host_mq'), port=5672, virtual_host=os.getenv('virtualhost_mq'), credentials=self.credentials)
        self.exchange = "modelo_exchange"
        self.routing_key = ""
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
    
    def send_message(self, body: dict):
        self.channel.basic_publish(exchange=self.exchange, body=json.dumps(body), properties=pika.BasicProperties(delivery_mode=2), routing_key=self.routing_key)

        
rabbitmq_publisher = RabbitmqPublisher()
dicionario = {'identificacao':'joao da silva', 'idade': '35'}

rabbitmq_publisher.send_message(dict=dicionario)


    
    