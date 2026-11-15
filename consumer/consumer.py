import os, json, time, logging, pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CONSUMER] %(message)s")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
EXCHANGE_NAME = "calc_exchange"
DLX_NAME = "calc_dlx"

def calculate(op, a, b):
    if op == "add": return a + b
    if op == "sub": return a - b
    if op == "mul": return a * b
    if op == "div": return a / b
    raise ValueError(f"Unknown operation: {op}")

def on_message(channel, method, properties, body):
    try:
        task = json.loads(body)
        logging.info(f"Received task: {task}")
        result = calculate(task["operation"], task["a"], task["b"])
        logging.info(f"RESULT task #{task['task_id']}: {task['a']} {task['operation']} {task['b']} = {result}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except ZeroDivisionError:
        logging.error(f"FAILED (division by zero): {body}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        logging.error(f"FAILED ({e}): {body}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    for attempt in range(30):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            break
        except pika.exceptions.AMQPConnectionError:
            time.sleep(2)
    channel = connection.channel()
    
    # Инфраструктура (Exchange, Очереди, DLQ)
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=DLX_NAME, exchange_type="fanout", durable=True)
    channel.queue_declare(queue="calc_dlq", durable=True)
    channel.queue_bind(queue="calc_dlq", exchange=DLX_NAME)
    channel.queue_declare(queue="calc_queue", durable=True, arguments={"x-dead-letter-exchange": DLX_NAME})
    channel.queue_bind(queue="calc_queue", exchange=EXCHANGE_NAME, routing_key="calc.*")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="calc_queue", on_message_callback=on_message)
    logging.info("Consumer started...")
    channel.start_consuming()

if __name__ == "__main__":
    main()