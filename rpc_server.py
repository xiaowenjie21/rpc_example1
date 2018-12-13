import pika, json
import threading
import time
from pika.exceptions import ChannelClosed
from pika.exceptions import ConnectionClosed


class RabbitMqServer:
    _instance_lock = threading.Lock()

    def __init__(self):
        self.connection = None

    def reconnect(self):
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()

            credit = pika.PlainCredentials('other', 'other')
            self.connection = pika.BlockingConnection(pika.ConnectionParameters('183.63.144.170', credentials=credit))
            self.channel = self.connection.channel()

            if isinstance(self, RpcServer):
                self.channel.exchange_declare(exchange='rpc', exchange_type='direct', auto_delete=False)
                self.channel.queue_declare(queue='pings', auto_delete=True)
                self.channel.queue_bind(queue='pings', exchange='rpc', routing_key='pings')
                self.channel.basic_consume(self.api_ping, queue='pings', consumer_tag='pings')

        except Exception as e:
            print('reconnect error: ', e)


class RpcServer(RabbitMqServer):
    def __init__(self):
        super(RpcServer, self).__init__()

    def fib(self, n):
        if n == 0:
            return 0
        elif n == 1:
            return 1
        else:
            return self.fib(n - 1) + self.fib(n - 2)

    def chen(self, n):
        return n * n

    def start(self):
        while True:
            try:
                self.reconnect()
                self.channel.start_consuming()

            except ConnectionClosed as e:
                self.reconnect()
                time.sleep(2)
                print('Connection closes error: ', e)

            except ChannelClosed as e:
                self.reconnect()
                time.sleep(2)
                print('channel close error: ', e)

            except Exception as e:
                self.reconnect()
                time.sleep(2)
                print('exception error: ', e)

    @classmethod
    def run(cls):
        rpc_server = cls()
        rpc_server.start()


    def api_ping(self, channel, method, header, body):
        channel.basic_ack(delivery_tag=method.delivery_tag) # 应答客户端
        print('我接受了来自客户端的消息{}, 正在进行计算'.format(int(body.decode())))
        result = self.chen(int(body.decode()))
        print('计算出结果{}，进行回复'.format(result))
        channel.basic_publish(body = str(result), exchange = '', routing_key = header.reply_to)


if __name__ == '__main__':
    t1 = threading.Thread(target = RpcServer.run).start()
    t2 = threading.Thread(target = RpcServer.run).start()