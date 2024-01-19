from pika import *
from datetime import date
import json
import pika
import os
from dotenv import load_dotenv
from botcity.web import WebBot

load_dotenv('app/config/.env')

class Modelo:
    def teste(self, identificacao, idade):
        print(f'O nome é: {identificacao}')
        print(f'A idade é: {idade}')
        
        

class RabbitmqConsumer():
    #*Cria a conexão com RabbitMQ
    def __init__(self):
        self.credentials = pika.PlainCredentials(username=os.getenv('user_mq'), password=os.getenv('senha_mq'))
        self.parameters = pika.ConnectionParameters(host=os.getenv('host_mq'), port=5672, virtual_host=os.getenv('virtualhost_mq'), credentials=self.credentials)  
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='modelo_consumer',passive=False,durable=True, exclusive=False, auto_delete=False)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue='modelo_consumer', on_message_callback=self.callback)
        self.bot = WebBot()
        
    def callback(self, ch, method, properties, body):
        # Transforma o JSON em um dicionário
        data_dict = json.loads(body.decode())
        
        processamento = self.process_data(data_dict)
        #*Se o robo_exec rodou, retira o item da fila
        if processamento: 
            ch.basic_ack(delivery_tag=method.delivery_tag)  
            
        else:
            print('erro na consulta do processo')

    #*URL aberta, o robo entra em execução com os dados retornados do RabbitMQ
    def process_data(self, data_dict):
        try:
            Modelo.teste(self, data_dict['identificacao'], data_dict['idade'])
            return True

        except AttributeError as atr:
            print(f'Attributeerror: {atr}, verifique o código!')
            pass

    def start_consuming(self):
        print('Aguardando mensagens...')
        self.channel.start_consuming()


consumidor = RabbitmqConsumer()
consumidor.start_consuming()

                

    