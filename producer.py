from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime

API_KEY = "My API Key"
URL = "https://www.goldapi.io/api/XAU/USD"

headers = {
    "x-access-token": API_KEY,
    "Content-Type": "application/json"
}

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Streaming Gold prices to Kafka!")

while True:
    response = requests.get(URL, headers=headers)
    data = response.json()

    message = {
        "metal": "gold",
        "price": data["price"],
        "currency": "USD",
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send("gold_prices", message)
    print("Sent:", message)

    time.sleep(3)
