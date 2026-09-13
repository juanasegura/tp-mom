import pika
import random
import string
from .middleware import (MessageMiddlewareQueue, MessageMiddlewareExchange,
                         MessageMiddlewareMessageError,
                         MessageMiddlewareDisconnectedError,
                         MessageMiddlewareCloseError)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        self.connection = connection
        self.channel = channel
        self.queue_name = queue_name
        self._is_consuming = False
        self._on_message_callback = None

    def start_consuming(self, on_message_callback):
        self._on_message_callback = on_message_callback
        self._is_consuming = True
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name,
                                       on_message_callback=self._handle_message)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()
        finally:
            self._is_consuming = False

    def _handle_message(self, channel, method, properties, body):
        def ack():
            channel.basic_ack(delivery_tag=method.delivery_tag)
        def nack():
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        self._on_message_callback(body, ack, nack)

    def stop_consuming(self):
        if not self._is_consuming:
            return
        try:
            self.channel.stop_consuming()
            self._is_consuming = False
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                                  routing_key=self.queue_name,
                                  body=message,
                                  properties=pika.BasicProperties(
                                      delivery_mode = pika.DeliveryMode.Persistent
                                  ))
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def close(self):
        self.stop_consuming()
        try:
            if self.connection.is_open:
                self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def send(self, message):
        pass

    def close(self):
        pass