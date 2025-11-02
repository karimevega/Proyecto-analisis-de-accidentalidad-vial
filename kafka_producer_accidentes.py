import time
import json
import random
from kafka import KafkaProducer

def generate_accidente_data():
    gravedades = ["Con heridos", "Con muertos", "Solo daños"]
    clases = ["Atropello", "Choque", "Volcamiento", "Caída ocupante"]
    
    return {
        "gravedad": random.choice(gravedades),
        "clase": random.choice(clases),
        "heridos": random.randint(0, 5),
        "muertos": random.randint(0, 2),
        "timestamp": int(time.time())
    }

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    accidente_data = generate_accidente_data()
    producer.send('accidentes_tiempo_real', value=accidente_data)
    print(f"Sent: {accidente_data}")
    time.sleep(1)
