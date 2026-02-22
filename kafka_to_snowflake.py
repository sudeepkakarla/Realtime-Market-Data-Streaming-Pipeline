from kafka import KafkaConsumer
import snowflake.connector
import json

# Kafka consumer
consumer = KafkaConsumer(
    "gold_prices",
    "fx_rates",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# Snowflake connection
conn = snowflake.connector.connect(
    user="MyUserName",
    password="MyPassword",
    account="buc96894.us-east-1",
    warehouse="COMPUTE_WH",
    database="MARKET_DATA",
    schema="RAW"
)

cursor = conn.cursor()

print("Consuming from Kafka and inserting into Snowflake...")

for message in consumer:
    data = message.value

    if message.topic == "gold_prices":
        cursor.execute(
            """
            INSERT INTO GOLD_PRICES (METAL, PRICE, CURRENCY, EVENT_TIME)
            VALUES (%s, %s, %s, %s)
            """,
            (
                data["metal"],
                data["price"],
                data["currency"],
                data["timestamp"]
            )
        )

    elif message.topic == "fx_rates":
        cursor.execute(
            """
            INSERT INTO FX_RATES (CURRENCY, RATE, EVENT_TIME)
            VALUES (%s, %s, %s)
            """,
            (
                data["currency"],
                data["rate"],
                data["timestamp"]
            )
        )

    conn.commit()
    print("Inserted:", data)
