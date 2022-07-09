import json
import os

import parallel_job_execution.rabbitmq.publisher


class ParallelPublisher(parallel_job_execution.rabbitmq.publisher.Publisher):
    def process_files(self):
        for file in ['file1.jpg', 'file2.jpg', 'file3.jpg']:
            self.publish(json.dumps({
                'method': 'process_file',
                'file': file
            }))


if __name__ == "__main__":
    publisher = ParallelPublisher(
        os.environ['EXCHANGE'],
        host=os.environ['RABBITMQ_HOST'],
        username=os.environ['RABBITMQ_USERNAME'],
        password=os.environ['RABBITMQ_PASSWORD']
    )
    publisher.process_files()
