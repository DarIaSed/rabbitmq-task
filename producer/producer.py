import os, time, json, random, logging, pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PRODUCER] %(message)s")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
EXCHANGE_NAME = "calc_exchange"

def main():
    for attempt in range(30):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            break
        except pika.exceptions.AMQPConnectionError:
            time.sleep(2)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)

    task_id = 0
    while True:
        task_id += 1
        op = random.choice(["add", "sub", "mul", "div"])
        a, b = random.randint(1, 100), random.randint(0, 100)
        payload = {"task_id": task_id, "operation": op, "a": a, "b": b}
        
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=f"calc.{op}",
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        logging.info(f"Sent task #{task_id}: {payload}")
        time.sleep(3)

if __name__ == "__main__":
    main()