import json
from pathlib import Path

from pika_client import PicaClient
from sql_queries import SqlRunner


def callback(ch, method, properties, body):
    print(" [x] Received %r" % json.loads(body))
    try:
        path = json.loads(body)['path']
        file_type = json.loads(body)['type']
        sql = SqlRunner(Path(path), file_type)
        sql.start_process()
    except KeyError:
        print("Parameter not provided, Consumer meed 'path' and 'type' params")


if __name__ == '__main__':
    p = PicaClient()
    p.channel.basic_consume(callback,
                            queue='sql_queue',
                            no_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    p.channel.start_consuming()
