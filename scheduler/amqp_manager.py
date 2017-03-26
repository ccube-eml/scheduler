import json

import pika


class AMQPManager(object):
    """
    Implements a manager for the AMQP protocol.
    """

    def __init__(self, hostname):
        """
        Initializes a manager connecting to the AMQP message broker.

        :param hostname: the hostname of the message broker
        :type hostname: str
        """
        self.__connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=hostname),
        )
        self.__channels = {}

    def close(self):
        for _, channel in self.__channels.items():
            channel.close()
        self.__connection.close()

    def create_queue(
            self,
            queue_name,
    ):
        """
        Creates a queue.

        :param queue_name: the queue name
        :type queue_name: str
        """
        channel = self.__get_channel(queue_name)

        channel.queue_declare(
            queue=queue_name,
            durable=True,
        )

    def queue_exists(
            self,
            queue_name,
    ):
        """
        Checks if a queue exists.

        :param queue_name: the queue name
        :type queue_name: str

        :return: True if exists, False otherwise
        :rtype: bool
        """
        channel = self.__get_channel(queue_name)

        try:
            channel.queue_declare(
                queue=queue_name,
                passive=True,
            )
            return True
        except Exception:
            return False

    def queue_size(
            self,
            queue_name,
    ):
        """
        Retrieves the number of messages in the queue.

        :param queue_name: the queue name
        :type queue_name: str

        :return: the number of messages in the queue
        :rtype: int
        """
        channel = self.__get_channel(queue_name)

        method_frame = channel.queue_declare(
            queue=queue_name,
            passive=True,
        )
        return method_frame.method.message_count

    def delete_queue(
         self,
         queue_name,
    ):
        """
        Deletes a queue.

        :param queue_name: the queue name
        :type queue_name: str
        """
        channel = self.__get_channel(queue_name)

        channel.queue_delete(
            queue=queue_name,
        )

    def publish_messages(
            self,
            queue_name,
            messages,
    ):
        """
        Publishes a message on the queue.

        :param queue_name: the name of the queue
        :type queue_name: str

        :param messages: the list of the messages to publish
        :type messages: list[object]
        """
        channel = self.__get_channel(queue_name)

        for message in messages:
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    content_type='application/json',
                    delivery_mode=2  # Persistent message,
                ),
            )

    def consume_messages(
            self,
            queue_name,
            messages_number,
    ):
        """
        Consumes the messages from the queue.

        :param queue_name: the queue name
        :type queue_name: str

        :param messages_number: the number of messages to consume
        :type messages_number: int

        :return: the messages and the delivery tags
        :rtype: (list[dict], list[str])
        """
        channel = self.__get_channel(queue_name)

        channel.basic_qos(prefetch_count=messages_number)

        messages = []
        delivery_tags = []

        messages_count = 0
        for method_frame, properties, body in channel.consume(queue=queue_name):
            delivery_tags.append(method_frame.delivery_tag)
            messages.append(json.loads(body.decode()))
            messages_count += 1
            if messages_count >= messages_number:
                break

        return messages, delivery_tags

    def acknowledge_messages(
            self,
            queue_name,
            delivery_tags,
    ):
        """
        Acknowledges the messages.

        :param queue_name: the queue name
        :type queue_name: str

        :param delivery_tags: the delivery tags for the acknowledgement
        :type delivery_tags: list[str]
        """
        channel = self.__get_channel(queue_name)

        for delivery_tag in delivery_tags:
            channel.basic_ack(delivery_tag=delivery_tag)

    def __get_channel(self, queue_name):
        """
        Gets the channel for the queue.

        :param queue_name: the queue name
        :type queue_name: str

        :return: the queue channel
        :rtype: pika.synchronous_connection.BlockingChannel
        """
        if queue_name not in self.__channels:
            self.__channels[queue_name] = self.__connection.channel()
        return self.__channels[queue_name]
