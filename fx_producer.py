from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime

API_KEY = "My API KEY"
URL = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"

TARGET_CURRENCIES = ["INR", "CNY", "AED"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Streaming selected FX rates...")

while True:
    response = requests.get(URL)
    data = response.json()

    timestamp = datetime.utcnow().isoformat()
    rates = data["conversion_rates"]

    for currency in TARGET_CURRENCIES:

        if currency in rates:

            message = {
                "currency": currency,
                "rate": rates[currency],
                "timestamp": timestamp
            }

            producer.send("fx_rates", message)
            print("Sent FX:", message)

    time.sleep(10)
