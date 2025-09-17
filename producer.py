# python -m venv venv
# venv\Scripts\activate
# pip freeze > requirements.txt
import os
import time
import random
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


# ---------- Cargar variables de entorno ----------
load_dotenv()


# ---------- Callback de confirmación ----------
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"✅ Record {msg.key()} produced to {msg.topic()} "
            f"[{msg.partition()}] at offset {msg.offset()}"
        )


# ---------- Configuración Kafka + Schema Registry ----------
kafka_config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),
}

schema_registry_client = SchemaRegistryClient({
    'url': os.getenv("SCHEMA_REGISTRY_URL"),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_KEY')}:{os.getenv('SCHEMA_REGISTRY_SECRET')}"
})

# Topic y Subject dinámicos
topic_name = os.getenv("KAFKA_TOPIC")
subject_name = os.getenv("SCHEMA_SUBJECT")

# Obtener Avro schema desde el Schema Registry
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Serializadores
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Producer
producer_conf = {
    **kafka_config,
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
}
producer = SerializingProducer(producer_conf)


# ---------- Función para producir valores pseudoaleatorios según el esquema ----------
def produce_data(n_messages=10, delay=1):
    shipment_status = ["pending", "in_transit", "delivered", "cancelled", "returned"]
    cities = ["New York, NY", "Chicago, IL", "Houston, TX", "Los Angeles, CA", "Miami, FL"]

    for i in range(n_messages):
        shipment_id = f"SH{random.randint(100000, 999999)}"
        origin, destination = random.sample(cities, 2)  # asegura que sean distintas
        status = random.choice(shipment_status)

        # timestamp: simulamos horas atrás
        timestamp = int(
            (datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 60 * 24)))
            .timestamp() * 1000
        )

        value = {
            "shipment_id": shipment_id,
            "origin": origin,
            "destination": destination,
            "status": status,
            "timestamp": timestamp
        }

        producer.produce(
            topic=topic_name,
            key=shipment_id,
            value=value,
            on_delivery=delivery_report
        )

        producer.poll(0)  # para procesar callbacks
        print(f"📦 Evento generado: {value}")
        time.sleep(delay)

    producer.flush()
    print("🚀 Todos los mensajes fueron enviados.")


if __name__ == "__main__":
    produce_data()