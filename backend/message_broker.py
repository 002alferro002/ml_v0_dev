import asyncio
import logging
import json
import aio_pika
from typing import Dict, Any, Callable, Optional
from exceptions import MessageBrokerError

logger = logging.getLogger(__name__)

class MessageBroker:
    def __init__(self, rabbitmq_url: str):
        self.rabbitmq_url = rabbitmq_url
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.is_connected = False
        self.reconnect_task: Optional[asyncio.Task] = None
        self.consumers: Dict[str, Callable] = {} # queue_name -> callback
        # logger.info("MessageBroker initialized.")

    async def connect(self):
        """Устанавливает соединение с RabbitMQ."""
        if self.is_connected:
            logger.warning("Already connected to RabbitMQ.")
            return

        try:
            self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=10) # Limit unacknowledged messages
            self.is_connected = True
            # logger.info("Connected to RabbitMQ.")
            
            # Re-establish consumers after reconnection
            for queue_name, callback in self.consumers.items():
                await self._start_consumer(queue_name, callback)

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
            self.is_connected = False
            raise MessageBrokerError(f"Failed to connect to RabbitMQ: {e}")

    async def disconnect(self):
        """Закрывает соединение с RabbitMQ."""
        if self.connection and not self.connection.is_closed:
            try:
                await self.connection.close()
                # logger.info("Disconnected from RabbitMQ.")
            except Exception as e:
                logger.error(f"Error during RabbitMQ disconnect: {e}", exc_info=True)
            finally:
                self.is_connected = False
                self.connection = None
                self.channel = None

    async def declare_queue(self, queue_name: str):
        """Объявляет очередь в RabbitMQ."""
        if not self.is_connected or not self.channel:
            logger.warning(f"Not connected to RabbitMQ, cannot declare queue {queue_name}.")
            raise MessageBrokerError("Not connected to RabbitMQ.")
        try:
            queue = await self.channel.declare_queue(queue_name, durable=True)
            # logger.info(f"Declared queue: {queue_name}")
            return queue
        except Exception as e:
            logger.error(f"Failed to declare queue {queue_name}: {e}", exc_info=True)
            raise MessageBrokerError(f"Failed to declare queue {queue_name}: {e}")

    async def publish(self, queue_name: str, message: Dict[str, Any]):
        """Публикует сообщение в указанную очередь."""
        if not self.is_connected or not self.channel:
            logger.warning(f"Not connected to RabbitMQ, cannot publish to {queue_name}. Message: {message}")
            raise MessageBrokerError("Not connected to RabbitMQ.")
        try:
            await self.channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(message).encode()),
                routing_key=queue_name
            )
            logger.debug(f"Published message to {queue_name}")
        except Exception as e:
            logger.error(f"Failed to publish message to {queue_name}: {e}", exc_info=True)
            raise MessageBrokerError(f"Failed to publish message to {queue_name}: {e}")

    async def consume(self, queue_name: str, callback: Callable[[Dict[str, Any]], None]):
        """Начинает потребление сообщений из указанной очереди."""
        self.consumers[queue_name] = callback # Store callback for reconnection
        if not self.is_connected or not self.channel:
            logger.warning(f"Not connected to RabbitMQ, cannot start consuming from {queue_name}.")
            raise MessageBrokerError("Not connected to RabbitMQ.")
        
        await self._start_consumer(queue_name, callback)

    async def _start_consumer(self, queue_name: str, callback: Callable[[Dict[str, Any]], None]):
        """Internal method to start a consumer."""
        try:
            queue = await self.channel.declare_queue(queue_name, durable=True) # Durable queue
            # logger.info(f"Declared queue: {queue_name}")

            async def process_message(message: aio_pika.IncomingMessage):
                async with message.process():
                    try:
                        data = json.loads(message.body.decode())
                        await callback(data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode message from {queue_name}: {message.body.decode()}. Error: {e}", exc_info=True)
                    except Exception as e:
                        logger.error(f"Error processing message from {queue_name}: {e}", exc_info=True)

            await queue.consume(process_message)
            # logger.info(f"Started consuming from queue: {queue_name}")
        except Exception as e:
            logger.error(f"Failed to start consumer for {queue_name}: {e}", exc_info=True)
            raise MessageBrokerError(f"Failed to start consumer for {queue_name}: {e}")


