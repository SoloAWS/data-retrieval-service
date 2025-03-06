# Arquitectura de Procesamiento de Comandos

## Visión General

El Servicio de Recuperación de Datos ahora admite una arquitectura híbrida:

1. **Procesamiento de Comandos vía Pulsar**: Comandos como crear tareas o iniciar tareas se reciben a través de un tópico de Pulsar.
2. **Procesamiento de Consultas vía HTTP**: Consultas como obtener detalles de tareas o recuperar imágenes se manejan a través de endpoints HTTP directos.
3. **Carga de Archivos vía HTTP**: Debido a las limitaciones de datos binarios, las cargas de imágenes todavía se procesan a través de HTTP.

## Flujo de Comandos

1. El servicio BFF envía comandos al tópico `persistent://public/default/data-retrieval-commands`.
2. El consumidor Pulsar del Servicio de Recuperación de Datos escucha este tópico.
3. Cuando se recibe un comando, se enruta al manejador apropiado según su tipo.
4. El manejador procesa el comando utilizando la lógica de negocio existente.
5. Los eventos del procesamiento de comandos se publican en sus respectivos tópicos.

## Tipos de Comandos

Se admiten los siguientes comandos:

1. **CreateRetrievalTask**: Crea una nueva tarea de recuperación de datos.
2. **StartRetrievalTask**: Inicia una tarea de recuperación de datos existente.
3. **UploadImage**: (No recomendado para Pulsar debido a datos binarios) Utilizado principalmente para pruebas.

## Componentes

### Consumidor Pulsar (PulsarConsumer)

- Ubicado en `src/data_retrieval_service/modules/data_retrieval/infrastructure/messaging/pulsar_consumer.py`
- Se suscribe a los tópicos de comandos
- Enruta los comandos a los manejadores apropiados
- Gestiona los acknowledgments y la lógica de reintentos

### Manejadores de Comandos

- Ubicados en `src/data_retrieval_service/modules/data_retrieval/application/commands/command_handlers.py`
- Mapean los comandos a la lógica de negocio existente
- Validan los datos del comando
- Devuelven resultados al llamador

## Configuración

El consumidor Pulsar se configura en `src/data_retrieval_service/config/settings.py`:

```python
# Configuración del consumidor Pulsar
pulsar_subscription_name: str = "data-retrieval-service"
pulsar_consumer_topics: list = ["persistent://public/default/data-retrieval-commands"]
pulsar_consumer_max_workers: int = 5
pulsar_consumer_batch_size: int = 10
# Configuración adicional...
```

## Consideraciones de Despliegue

1. **Escalabilidad**: El consumidor puede escalarse horizontalmente ejecutando múltiples instancias del servicio.
2. **Tolerancia a Fallos**: Los comandos fallidos se reintentan utilizando el mecanismo de reconocimiento negativo incorporado en Pulsar.
3. **Monitoreo**: El endpoint de salud incluye el estado del consumidor para su monitoreo.

## Manejo de Errores

1. **Validación de Comandos**: Los comandos se validan antes de procesarse.
2. **Registro de Errores**: Se producen registros de errores detallados para la solución de problemas.
3. **Recuperación de Errores**: El consumidor incluye recuperación de errores para mantener la estabilidad del servicio.

## Diagrama de Flujo

```
+-------+    Comando     +-------------------+    Procesamiento    +---------------+
|  BFF  | ------------> | Tópico de Pulsar  | -----------------> | Data Retrieval |
+-------+               +-------------------+                     +---------------+
                                                                          |
                                                                          |
+-------+    Consulta     +---------------+                               |
|  BFF  | -------------> | Data Retrieval |                               |
+-------+                +---------------+                                |
                                                                          |
                                                                          V
                                                       +-----------------------------------+
                                                       | Base de Datos + Sistema de Archivos |
                                                       +-----------------------------------+
```

## Beneficios del Enfoque

1. **Desacoplamiento**: BFF y Data Retrieval están desacoplados a través de Pulsar, permitiendo mayor flexibilidad.
2. **Resiliencia**: Los mensajes se almacenan en Pulsar hasta que se procesen correctamente.
3. **Escalabilidad**: Múltiples instancias pueden procesar comandos en paralelo.
4. **Trazabilidad**: Cada comando tiene un ID único y puede ser rastreado a través del sistema.

## Configuración de Pulsar

Para configurar Pulsar correctamente:

1. Asegúrese de que Pulsar esté ejecutándose y sea accesible desde el servicio.
2. Defina las variables de entorno apropiadas:
   - `PULSAR_SERVICE_URL`
   - `PULSAR_TOKEN` (si se requiere autenticación)
   - `PULSAR_SUBSCRIPTION_NAME`

## Pruebas

Para probar el procesamiento de comandos:

1. Envíe un comando al tópico de Pulsar utilizando el BFF.
2. Verifique los registros para la recepción y procesamiento del comando.
3. Consulte el estado de la tarea utilizando los endpoints HTTP para confirmar el procesamiento exitoso.

## Solución de Problemas

Si los comandos no se procesan:

1. Verifique que el servicio tenga conectividad con Pulsar.
2. Revise los registros del servicio para mensajes de error.
3. Confirme que el tópico correcto está configurado tanto en el BFF como en el servicio de Data Retrieval.
4. Verifique que el formato del comando sea correcto.

## Ampliaciones Futuras

1. **Monitoreo Avanzado**: Integración con sistemas de monitoreo para seguimiento detallado.
2. **Manejo de Errores Mejorado**: Implementación de DLQ (Dead Letter Queue) para comandos fallidos.
3. **Más Tipos de Comandos**: Adición de comandos adicionales a medida que evoluciona el sistema.
4. **Procesamiento en Lotes**: Optimización del rendimiento mediante procesamiento en lotes.

## Conclusión

Esta arquitectura híbrida de CQRS (Command Query Responsibility Segregation) proporciona un equilibrio entre robustez para los comandos y simplicidad para las consultas, al tiempo que mantiene la compatibilidad con el manejo existente de archivos binarios.