import json
import os

from pika_client import PicaClient

p = PicaClient()
p.channel.basic_publish(exchange='',
                        routing_key='sql_queue',
                        body=json.dumps(
                            {'type': 'csv', 'path': os.path.dirname(__file__) + '{}chinook.db'.format(os.sep)}))
print(" [x] Sent json to sql_queue")
p.close_con()
