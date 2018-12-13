import time, json, pika
import threading
from pika.exceptions import ChannelClosed
from pika.exceptions import ConnectionClosed


class RabbitMQServer:
    _thread_lock = threading.Lock()

    def __init__(self):
        self.connection = None
        self.msg = ''
        self.i = 1

    def reconnet(self):
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()

            credit = pika.PlainCredentials('other', 'ohter')
            self.connection = pika.BlockingConnection(pika.ConnectionParameters('183.63.144.170', credentials=credit))
            self.channel = self.connection.channel()

            if isinstance(self, RpcClient):
                # 从多线程中传送过来的消息进行处理，并发布到rpc服务器, exclusive 确保唯一队列
                result = self.channel.queue_declare(exclusive = True, auto_delete = True)
                self.msg_props = pika.BasicProperties()
                self.msg_props.reply_to = result.method.queue
                print('我将发送消息{}到rpc服务器'.format(self.msg))
                self.channel.basic_publish(body = str(self.msg), exchange='rpc', properties = self.msg_props, routing_key='pings')
                self.channel.basic_consume(self.replay_callback, queue = result.method.queue, consumer_tag=result.method.queue)

        except Exception as e:
            print('reconnect error: ', e)


class RpcClient(RabbitMQServer):
    def __init__(self):
        super(RpcClient, self).__init__()
        self.body = None


    def replay_callback(self, channel, method, header, body):
        """接受两轮回复消息"""
        print(self.i)
        if self.i >= 2:
            self.channel.stop_consuming()
        print('收到的应答结果是 {}'.format(body.decode()))
        self.body = body
        self.new_body = int(body.decode()) * 10
        print('将应答消息x2再次发送给服务器')
        self.channel.basic_publish(body = str(self.new_body), exchange='rpc', properties = self.msg_props, routing_key='pings')
        self.i += 1

    def start(self):
        try:
            # if self.body is not None:
            #     print('body 是: {}'.format(self.body))
            #     self.channel.stop_consuming()
            # else:
            self.reconnet()
            self.channel.start_consuming()
        except ConnectionClosed as e:
            print('error connection closed: ', e)
            self.reconnet()
            time.sleep(2)
        except ChannelClosed as e:
            print('error channel closed: ', e)
            self.reconnet()
            time.sleep(2)
        except Exception as e:
            print('error exception error: ', e)
            self.reconnet()
            time.sleep(2)


    @classmethod
    def run(cls, msg):
        rpc_client = cls()
        rpc_client.msg = msg
        rpc_client.start()



if __name__ == '__main__':
    for i in range(5):
        threading.Thread(target = RpcClient.run, args = (i, )).start()

