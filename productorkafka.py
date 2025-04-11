import time
import json
import random
from kafka import KafkaProducer

def generador_datos_tienda():
    return {
        "id_tienda": random.randint(1, 30),
        "transacciones": random.randint(1, 20),
        "total": round(random.uniform(1, 100000), 2),
        "timestamp": int(time.time())
    }

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    datos_tienda = generador_datos_tienda()
    producer.send('datos_tienda', value=datos_tienda)
    print(f"Sent: {datos_tienda}")
    time.sleep(1)
