# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import logging
import os
import threading
import time
import pika
from pika.exchange_type import ExchangeType

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


class Consumer:
    def __init__(self, exchange, queue=None, routing_key=None, host='localhost', username='guest', password='guest'):
        self.threads = []
        self.host = host
        self.exchange = exchange
        self.queue = queue if queue is not None else self.exchange
        self.routing_key = routing_key if routing_key is not None else self.exchange

        credentials = pika.PlainCredentials(username, password)
        # Note: sending a short heartbeat to prove that heartbeats are still
        # sent even though the worker simulates long-running work
        parameters = pika.ConnectionParameters(
            host, credentials=credentials, heartbeat=5)
        self.connection = pika.BlockingConnection(parameters)

        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type=ExchangeType.direct,
            passive=False,
            durable=True,
            auto_delete=False)
        self.channel.queue_declare(queue=self.queue, auto_delete=True, durable=True)
        self.channel.queue_bind(
            queue=self.queue, exchange=self.exchange, routing_key=self.routing_key)
        # Note: prefetch is set to 1 here as an example only and to keep the number of threads created
        # to a reasonable amount. In production you will want to test with different prefetch values
        # to find which one provides the best performance and usability for your solution
        self.channel.basic_qos(prefetch_count=1)

    def start(self):
        on_message_callback = functools.partial(self.on_message, args=(self.connection, self.threads))
        self.channel.basic_consume(self.queue, on_message_callback)

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()

        # Wait for all to complete
        for thread in self.threads:
            thread.join()

        self.connection.close()

    def ack_message(self, ch, delivery_tag):
        """Note that `ch` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if ch.is_open:
            ch.basic_ack(delivery_tag)
        else:
            # Channel is already closed, so we can't ACK this message;
            # log and/or do something that makes sense for your app in this case.
            pass

    def do_work(self, conn, ch, delivery_tag, body):
        thread_id = threading.get_ident()
        LOGGER.info('Thread id: %s Delivery tag: %s Message body: %s', thread_id,
                    delivery_tag, body)

        self.process_task(body)

        cb = functools.partial(self.ack_message, ch, delivery_tag)
        conn.add_callback_threadsafe(cb)

    def on_message(self, ch, method_frame, _header_frame, body, args):
        (conn, thrds) = args
        delivery_tag = method_frame.delivery_tag
        t = threading.Thread(target=self.do_work, args=(conn, ch, delivery_tag, body))
        t.start()
        thrds.append(t)

    def process_task(self, body):
        print('Processed task')


if __name__ == "__main__":
    consumer = Consumer(
        os.environ['EXCHANGE'],
        host=os.environ['RABBITMQ_HOST'],
        username=os.environ['RABBITMQ_USERNAME'],
        password=os.environ['RABBITMQ_PASSWORD']
    )
    consumer.start()

