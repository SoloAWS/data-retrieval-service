import json
import logging
import asyncio
import pulsar
import uuid
import traceback
from typing import Dict, Any, List, Callable, Awaitable, Optional
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class PulsarConsumer:
    """
    Consumidor de mensajes usando Apache Pulsar.
    Se encarga de recibir y procesar comandos del BFF.
    """

    def __init__(
        self,
        service_url: str,
        subscription_name: str,
        topics: List[str],
        command_handlers: Dict[str, Callable],
        token: Optional[str] = None,
        consumer_config: Dict[str, Any] = None,
        publisher: Optional[Any] = None,
        max_workers: int = 5,
        get_unit_of_work_func: Optional[Callable] = None
    ):
        """
        Inicializa el consumidor de Pulsar

        Args:
            service_url: URL del servicio Pulsar
            subscription_name: Nombre de la suscripción
            topics: Lista de tópicos a los que suscribirse
            command_handlers: Diccionario que mapea tipos de comandos a sus manejadores
            token: Token de autenticación opcional
            consumer_config: Configuración adicional para el consumidor
            publisher: Instancia de PulsarPublisher para publicar eventos de respuesta
            max_workers: Número máximo de workers para procesamiento en paralelo
            get_unit_of_work_func: Función para obtener una nueva instancia de UnitOfWork
        """
        self.service_url = service_url
        self.subscription_name = subscription_name
        self.topics = topics
        self.token = token
        self.consumer_config = consumer_config or {}
        self.command_handlers = command_handlers
        self.publisher = publisher
        self.max_workers = max_workers
        self.get_unit_of_work_func = get_unit_of_work_func
        
        # Componentes inicializados bajo demanda
        self.client = None
        self.consumer = None
        self._is_running = False
        self._consumer_task = None
        self._executor = None
        
        # Contador para registro de eventos periódico
        self._timeout_counter = 0
        self._log_interval = 100  # Registrar cada 100 timeouts

    async def start(self):
        """Inicia el consumidor de Pulsar"""
        if self._is_running:
            logger.warning("Consumer is already running")
            return

        try:
            # Inicializar cliente Pulsar
            auth = None
            if self.token:
                auth = pulsar.AuthenticationToken(self.token)
            
            self.client = pulsar.Client(
                service_url=self.service_url,
                authentication=auth
            )
            
            # Verificar si hay múltiples tópicos o solo uno
            if len(self.topics) > 1:
                # Cuando hay múltiples tópicos, usamos topic (singular) con una lista
                self.consumer = self.client.subscribe(
                    topic=self.topics,  # Pulsar acepta una lista de tópicos aquí
                    subscription_name=self.subscription_name,
                    consumer_type=pulsar.ConsumerType.Shared,
                    **self.consumer_config
                )
            else:
                # Cuando hay un solo tópico, simplemente pasamos ese tópico
                self.consumer = self.client.subscribe(
                    topic=self.topics[0],
                    subscription_name=self.subscription_name,
                    consumer_type=pulsar.ConsumerType.Shared,
                    **self.consumer_config
                )
            
            logger.info(f"Pulsar consumer initialized for topics {self.topics}")

            # Crear executor de threads
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
            
            # Marcar como en ejecución
            self._is_running = True
            
            # Iniciar tarea de consumo
            self._consumer_task = asyncio.create_task(self._consume_messages())
            
            logger.info("Pulsar consumer task started")
        except Exception as e:
            logger.error(f"Error starting Pulsar consumer: {str(e)}")
            self.close()
            raise

    async def stop(self):
        """Detiene el consumidor de Pulsar"""
        if not self._is_running:
            return

        logger.info("Stopping Pulsar consumer...")
        self._is_running = False
        
        if self._consumer_task:
            try:
                # Esperar a que finalice la tarea de consumo
                await asyncio.wait_for(self._consumer_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Consumer task did not complete in time, cancelling")
                self._consumer_task.cancel()
            except Exception as e:
                logger.error(f"Error stopping consumer task: {str(e)}")
            finally:
                self._consumer_task = None
        
        self.close()
        logger.info("Pulsar consumer stopped")

    def close(self):
        """Cierra todas las conexiones y recursos"""
        # Cerrar executor
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None
        
        # Cerrar consumidor
        if self.consumer:
            try:
                self.consumer.close()
            except Exception as e:
                logger.warning(f"Error closing consumer: {str(e)}")
            finally:
                self.consumer = None
        
        # Cerrar cliente
        if self.client:
            try:
                self.client.close()
            except Exception as e:
                logger.warning(f"Error closing client: {str(e)}")
            finally:
                self.client = None

    async def _consume_messages(self):
        """
        Tarea principal para consumir mensajes de Pulsar.
        Se ejecuta continuamente hasta que se detiene el consumidor.
        """
        logger.info("Started consuming messages")
        last_msg_time = asyncio.get_event_loop().time()
        
        while self._is_running:
            try:
                # Recibir mensaje (operación bloqueante) con un timeout
                loop = asyncio.get_event_loop()
                msg_future = loop.run_in_executor(
                    self._executor, 
                    lambda: self.consumer.receive(timeout_millis=1000)
                )
                
                try:
                    # Esperar mensaje con timeout
                    msg = await asyncio.wait_for(msg_future, timeout=2.0)
                    
                    # Reiniciar contador de timeouts y registrar tiempo
                    self._timeout_counter = 0
                    last_msg_time = loop.time()
                    
                    # Procesar mensaje en tarea separada
                    asyncio.create_task(self._process_message(msg))
                except asyncio.TimeoutError:
                    # Timeout normal, solo incrementar contador
                    self._timeout_counter += 1
                    
                    # Registrar solo periódicamente para evitar inundar el log
                    if self._timeout_counter % self._log_interval == 0:
                        current_time = loop.time()
                        idle_time = current_time - last_msg_time
                        logger.debug(f"No new messages in {idle_time:.1f} seconds (timeout count: {self._timeout_counter})")
                    
                    # Verificar si debemos seguir ejecutando
                    continue
                except pulsar._pulsar.Timeout:
                    # Timeout de Pulsar, comportamiento similar
                    self._timeout_counter += 1
                    if self._timeout_counter % self._log_interval == 0:
                        logger.debug(f"Pulsar timeout occurred ({self._timeout_counter})")
                    continue
                except Exception as e:
                    # Solo registrar errores que no sean timeouts
                    if not isinstance(e, (asyncio.TimeoutError, pulsar._pulsar.Timeout)):
                        logger.error(f"Error receiving message: {str(e)}")
                    await asyncio.sleep(1.0)  # Pausa para evitar CPU 100%
            except asyncio.CancelledError:
                # La tarea fue cancelada, salir del bucle
                logger.info("Consumer task cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in consumer loop: {str(e)}")
                await asyncio.sleep(1.0)  # Pausa para evitar CPU 100%
        
        logger.info("Stopped consuming messages")

    async def _process_message(self, msg):
        """
        Procesa un mensaje recibido de Pulsar.
        
        Args:
            msg: Mensaje de Pulsar
        """
        command_id = "unknown"
        command_type = "unknown"
        
        try:
            # Decodificar el mensaje
            payload = msg.data().decode('utf-8')
            data = json.loads(payload)
            
            # Extraer información del comando
            command_type = data.get('type', 'unknown')
            command_id = data.get('id', 'unknown')
            correlation_id = data.get('correlation_id', 'unknown')
            command_data = data.get('data', {})
            
            logger.info(f"Received command: {command_type} (ID: {command_id})")
            
            # Verificar si existe un manejador para este tipo de comando
            if command_type in self.command_handlers:
                handler = self.command_handlers[command_type]
                
                # Obtener el unit of work para este comando
                uow = self.get_unit_of_work_func() if self.get_unit_of_work_func else None
                
                # Ejecutar el manejador de comando
                result = await handler(command_data, uow, self.publisher, correlation_id)
                
                logger.info(f"Command {command_type} (ID: {command_id}) processed successfully")
                
                # Acknowledgment del mensaje procesado
                self.consumer.acknowledge(msg)
            else:
                logger.warning(f"No handler found for command type: {command_type}")
                # Negative acknowledgment para que se reintente
                self.consumer.negative_acknowledge(msg)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding command message: {str(e)}")
            # Mensaje malformado, no intentar de nuevo
            self.consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error processing command {command_type} (ID: {command_id}): {str(e)}")
            logger.error(traceback.format_exc())
            # Negative acknowledgment para que se reintente
            self.consumer.negative_acknowledge(msg)