import pika
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange
from pika.exceptions import AMQPChannelError, AMQPConnectionError

class ConnectionManager:
    def __init__(self, host):
        self.host = host
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
        except (AMQPConnectionError, AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

    def validate_connection(self):
        if self.channel.is_closed or self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()

    def get_channel(self):
        return self.channel
        
class Consumer:
    def __init__(self, channel, queue_name):
        self.channel = channel
        self.queue_name = queue_name

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(body, lambda: ch.basic_ack(delivery_tag=method.delivery_tag), lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True))

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback) 
        try:
            self.channel.start_consuming()
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection_manager = ConnectionManager(host)
        self.consumer = Consumer(self.connection_manager.get_channel(), queue_name)
        try:
            channel = self.connection_manager.get_channel()
            channel.queue_declare(queue=queue_name, durable=True)
            self.queue = queue_name
        except (AMQPConnectionError, AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        self.connection_manager.close()

    def send(self, message):
        self.connection_manager.validate_connection()
        channel = self.connection_manager.get_channel()
        try:
            channel.basic_publish(exchange='', routing_key=self.queue, body=message, properties=pika.BasicProperties(delivery_mode=2)) # delivery_mode=2 para persistir los mensajes
        except (AMQPConnectionError, AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def start_consuming(self, on_message_callback):
        self.connection_manager.validate_connection()
        channel = self.connection_manager.get_channel()
        channel.basic_qos(prefetch_count=1) 
        self.consumer.start_consuming(on_message_callback)

    def stop_consuming(self):
        self.connection_manager.validate_connection()
        self.consumer.stop_consuming()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            self.connection_mannager = ConnectionManager(host)
            self.exchange_name = exchange_name
            self.routing_keys = routing_keys

            channel = self.connection_mannager.get_channel()
            channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
            retult = channel.queue_declare(queue='', exclusive=True)  

            self.queue = retult.method.queue
            self.consumer = Consumer(channel, self.queue)
            for routingkey in routing_keys:
                channel.queue_bind(exchange=exchange_name, queue=self.queue, routing_key=routingkey)
        except (AMQPConnectionError, AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
        
    def close(self):
        self.connection_mannager.close()

    def send(self, message):
        self.connection_mannager.validate_connection()
        channel = self.connection_mannager.get_channel()
        try:
            for routing_key in self.routing_keys:
                    channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
        except (AMQPConnectionError, AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def start_consuming(self, on_message_callback):
        self.connection_mannager.validate_connection()
        self.consumer.start_consuming(on_message_callback)

    def stop_consuming(self):
        self.connection_mannager.validate_connection()
        self.consumer.stop_consuming()