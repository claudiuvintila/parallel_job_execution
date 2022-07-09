# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
import json
import os
import logging
import pika
from pika.exchange_type import ExchangeType
from pika.spec import PERSISTENT_DELIVERY_MODE

logging.basicConfig(level=logging.WARN)


class Publisher:
    def __init__(self, exchange, exchange_type=ExchangeType.direct, host='localhost', username='guest', password='guest'):
        self.host = host
        self.exchange = exchange
        self.exchange_type = exchange_type
        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(host, credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange,
             exchange_type=self.exchange_type,
             passive=False,
             durable=True,
             auto_delete=False)

    def __del__(self):
        self.connection.close()

    def publish(self, msg, routing_key=None):
        if routing_key is None:
            routing_key = self.exchange
        print(self.exchange, routing_key, msg)
        print("Sending message to exchange: " + self.exchange)
        self.channel.basic_publish(
            self.exchange, routing_key, msg,
            pika.BasicProperties(content_type='text/plain',
                                 delivery_mode=PERSISTENT_DELIVERY_MODE))


if __name__ == "__main__":
    publisher = Publisher(
        os.environ['EXCHANGE'],
        queue='standard',
        host=os.environ['RABBITMQ_HOST'],
        username=os.environ['RABBITMQ_USERNAME'],
        password=os.environ['RABBITMQ_PASSWORD']
    )
    body = json.dumps({
        'db': 'dd',
        'backup': 'vv'
    })
    publisher.publish(body)


