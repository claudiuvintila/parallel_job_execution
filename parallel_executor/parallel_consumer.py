import json
import os

import parallel_job_execution.rabbitmq.consumer
import parallel_job_execution.parallel_executor.business


class ParallelConsumer(parallel_job_execution.rabbitmq.consumer.Consumer):
    def __init__(self, exchange, queue='standard', routing_key='standard_key', host='localhost', username='guest', password='guest'):
        super().__init__(exchange, queue, routing_key, host, username, password)

        self.business = parallel_job_execution.parallel_executor.business.Business()

    def process_task(self, body):
        obj = json.loads(body)

        if obj['method'] == 'process_file':
            self.business.process_file(obj['file'])


if __name__ == "__main__":
    consumer = ParallelConsumer(
        os.environ['EXCHANGE'],
        host=os.environ['RABBITMQ_HOST'],
        username=os.environ['RABBITMQ_USERNAME'],
        password=os.environ['RABBITMQ_PASSWORD']
    )
    consumer.start()
