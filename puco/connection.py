import pika
import logging


__all__ = ["PusherConsumer"]

class PusherConsumer(object):

    def __init__(self,
                 rabbitmq_user,
                 rabbitmq_password,
                 rabbitmq_server,
                 rabbitmq_port,
                 rabbitmq_vhost,
                 rabbitmq_exchange,
                 on_publish_channel_has_been_opend=None,
                 on_consum_channel_has_been_opend=None,
                 on_message_received=None,
                 on_connection_open_error=None,
                 on_connection_closed=None):

        """Creates a new PuCo instance.

        :param str rabbitmq_user: Username for RabbitMQ to connect
        :param str rabbitmq_password: Password for RabbitMQ to connect
        :param str rabbitmq_server: IP/Hostname for RabbitMQ to connect
        :param int rabbitmq_port: TCP port for RabbitMQ to connect
        :param str rabbitmq_vhost: Used virtual host in RabbitMQ
        :param str rabbitmq_exchange: Used Exchange in RabbitMQ
        :param callable on_publish_channel_has_been_opend: Called when a publish channel has been opened.
        :param callable on_consum_channel_has_been_opend: Called when a consum channel has been opened.
        :param callable on_message_received: Called when a message has been received
        :param callable on_connection_open_error: Called when the connection can't be established or the
            connection establishment is interrupted.
        :param callable on_connection_closed: Called when the connection is closed.

        """

        self.__rabbitmq_exchange = rabbitmq_exchange
        self.__rabbitmq_exchange_type = 'topic'

        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        parameters = pika.ConnectionParameters(rabbitmq_server,
                                               rabbitmq_port,
                                               rabbitmq_vhost,
                                               credentials)

        # Init Intern properties
        self.__publish_channel = None
        self.__consum_channel = None

        # Extern callback Events
        self.__ecb_publish_channel_has_been_opend = on_publish_channel_has_been_opend
        self.__ecb_consum_channel_has_been_opend = on_consum_channel_has_been_opend
        self.__ecb_message_received = on_message_received
        self.__ecb_connection_open_error = on_connection_open_error
        self.__ecb_connection_closed = on_connection_closed

        # Set connection parameters and callbacks
        self.__connection = pika.SelectConnection(parameters,
                                                  on_open_callback=self.__on_connection_open,
                                                  on_open_error_callback=self.__on_connection_open_error,
                                                  on_close_callback=self.__on_connection_closed)

    def connect(self):
        self.__connection.ioloop.start()

    def disconnect(self):
        self.__connection.ioloop.stop()

    # Connection Callbacks
    def __on_connection_open(self, connection):
        self.__connection.channel(on_open_callback=self.__on_publish_channel_open)
        self.__connection.channel(on_open_callback=self.__on_consum_channel_open)

        logging.info("[{}] {} -> {} ".format(__name__, "Connection open", connection))

    def __on_connection_open_error(self, connection, error):
        self.__callback(self.__ecb_connection_open_error, error)

        logging.error("[{}] {} -> {} | Error:{}".format(__name__, "Connection error", connection, error))

    def __on_connection_closed(self, connection, reason):
        self.__callback(self.__ecb_connection_closed, reason)

        self.__publish_channel = None
        self.__consum_channel = None

        logging.warning("[{}] {} -> {} | Reason:{}".format(__name__, "Connection closed", connection, reason))

    # Channels Callbacks
    def __on_publish_channel_open(self, channel):

        self.__publish_channel = channel
        self.__publish_channel.exchange_declare(exchange=self.__rabbitmq_exchange,
                                                exchange_type=self.__rabbitmq_exchange_type,
                                                durable=True)

        logging.info("[{}] {} -> {} ".format(__name__, "PublishChannel opened/exchange declared", channel))
        self.__callback(self.__ecb_publish_channel_has_been_opend)

    def __on_consum_channel_open(self, channel):

        self.__consum_channel = channel
        self.__consum_channel.exchange_declare(exchange=self.__rabbitmq_exchange,
                                               exchange_type=self.__rabbitmq_exchange_type,
                                               durable=True)

        logging.info("[{}] {} -> {} ".format(__name__, "ConsumChannel opened/exchange declared", channel))
        self.__callback(self.__ecb_consum_channel_has_been_opend)

    def __icb_message_received_inform_callback(self, channel, method, properties, body):

        self.__callback(self.__ecb_message_received, channel, method, properties, body)

    # Public Methods
    def register_consumer_queue_for_routing_key(self, queue, routing_key):

        self.__consum_channel.queue_declare(queue)
        self.__consum_channel.queue_bind(exchange=self.__rabbitmq_exchange,
                                         queue=queue,
                                         routing_key=routing_key)
        self.__consum_channel.basic_consume(queue=queue,
                                            on_message_callback=self.__icb_message_received_inform_callback)

        logging.info("[{}] {} -> {} | RoutingKey: {} ".format(__name__, "Queue created/bound it", queue, routing_key))

    def publish_message_for_routing_key(self, message, routing_key):
        logging.info("[{}] {} -> {} | RoutingKey: {}".format(__name__, "Publishing message", message, routing_key))

        self.__publish_channel.basic_publish(
            exchange=self.__rabbitmq_exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
               delivery_mode=2,
            ))

    @staticmethod
    def __callback(callback, *args):
        if callback:
            try:
                callback(*args)
                logging.debug("{}: {} {}".format(__name__, callback, args))
            except Exception as e:
                logging.error("{}: [Callback:{}] [Exception:{}] [Args:{}]".format(__name__, callback, e, args))