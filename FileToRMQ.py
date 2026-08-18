import os
import shutil
import time
import pika
import sys
import configparser
import logging
from logging.handlers import RotatingFileHandler

# ---------------------------------------------------------
# LOAD CONFIG
# ---------------------------------------------------------
def load_config(file_path="settings.ini"):
    config = configparser.ConfigParser()
    config.read(file_path)

    return {
        "paths": {
            "source": config.get("paths", "source_dir"),
            "processed": config.get("paths", "processed_dir"),
            "failed": config.get("paths", "failed_dir"),
        },
        "rabbitmq": {
            "host": config.get("rabbitmq", "host"),
            "port": config.getint("rabbitmq", "port"),
            "username": config.get("rabbitmq", "username"),
            "password": config.get("rabbitmq", "password"),
            "virtual_host": config.get("rabbitmq", "virtual_host", fallback="/"),
            "queue": config.get("rabbitmq", "queue"),
            "exchange": config.get("rabbitmq", "exchange", fallback=""),
            "exchange_type": config.get("rabbitmq", "exchange_type", fallback="direct"),
            "routing_key": config.get("rabbitmq", "routing_key"),
        },
        "logging": {
            "log_file": config.get("logging", "log_file", fallback="app.log"),
            "log_level": config.get("logging", "log_level", fallback="INFO"),
        }
    }


# ---------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------
def setup_logging(log_file, log_level):
    level = getattr(logging, log_level.upper(), logging.ERROR)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    )

    handler = RotatingFileHandler(
        log_file, maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    handler.setFormatter(formatter)

    logging.basicConfig(level=level, handlers=[handler])

# ---------------------------------------------------------
# DIRECTORY HANDLING
# ---------------------------------------------------------
def ensure_directories(paths):
    logging.info("Ensuring processed/failed directories exist")
    os.makedirs(paths["processed"], exist_ok=True)
    os.makedirs(paths["failed"], exist_ok=True)


def has_files(folder):
    return any(os.path.isfile(os.path.join(folder, f)) for f in os.listdir(folder))


def check_file(folder):
    logging.info(f"Checking for files in: {folder}")

    if not has_files(folder):
        logging.error(f"No files found in: {folder}")
        raise ValueError(f"No files found in: {folder}")

    logging.info(f"Files found in: {folder}")


# ---------------------------------------------------------
# RABBITMQ CONNECTION
# ---------------------------------------------------------
def connect_rmq(config):
    logging.info("Connecting to RabbitMQ...")

    credentials = pika.PlainCredentials(
        config["username"], config["password"]
    )

    parameters = pika.ConnectionParameters(
        host=config["host"],
        port=config["port"],
        virtual_host=config["virtual_host"],
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Declare exchange (if provided)
    if config["exchange"]:
        channel.exchange_declare(
            exchange=config["exchange"],
            exchange_type=config["exchange_type"],
            durable=True
        )

    # Declare queue
    channel.queue_declare(queue=config["queue"], durable=True)

    # Bind queue to exchange (if exchange exists)
    if config["exchange"]:
        channel.queue_bind(
            queue=config["queue"],
            exchange=config["exchange"],
            routing_key=config["routing_key"]
        )

    channel.confirm_delivery()

    logging.info(
        f"Connected to RMQ vhost '{config['virtual_host']}' "
        f"exchange '{config['exchange']}' routing_key '{config['routing_key']}'"
    )

    return connection, channel



def publish_message(channel, config, message):
    try:
        channel.basic_publish(
            exchange=config["exchange"],
            routing_key=config["routing_key"],
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        return True
    except Exception as e:
        logging.error(f"Publish failed: {e}")
        return False


# ---------------------------------------------------------
# FILE PROCESSING
# ---------------------------------------------------------
def is_file_ready(filepath):
    size1 = os.path.getsize(filepath)
    time.sleep(1)
    size2 = os.path.getsize(filepath)

    ready = size1 == size2
    if not ready:
        logging.warning(f"File still writing: {filepath}")

    return ready


def process_file(filepath, channel, config, paths):
    filename = os.path.basename(filepath)
    logging.info(f"Processing file: {filename}")

    if not is_file_ready(filepath):
        logging.info(f"Skipping (still writing): {filename}")
        return

    try:
        filepath = str(filepath)
        filename = str(filename)
        with open(filepath, "rb") as f:
            data = f.read()

        for attempt in range(3):
            if publish_message(channel, config, data):
                print(f"Sent to RMQ: {filename}")
                shutil.move(filepath, os.path.join(paths["processed"], filename))
                return
            else:
                print(f"Retry {attempt + 1} for {filename}")
                time.sleep(2)

        print(f"Failed permanently: {filename}")
        shutil.move(filepath, os.path.join(paths["failed"], filename))

    except Exception as e:
        logging.exception(f"Error processing {filename}: {e}")
        shutil.move(filepath, os.path.join(str(paths["failed"]), filename))


# ---------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------
def main():
    config = load_config()
    paths = config["paths"]
    rmq_config = config["rabbitmq"]
    log_cfg = config["logging"]

    setup_logging(log_cfg["log_file"], log_cfg["log_level"])

    ensure_directories(paths)

    try:
        check_file(paths["source"])
    except ValueError as error:
        logging.error(error)
        sys.exit(1)

    connection, channel = connect_rmq(rmq_config)

    logging.info("Processing files and sending to RMQ...")

    try:
        while True:
            files = [
                os.path.join(paths["source"], f)
                for f in os.listdir(paths["source"])
                if os.path.isfile(os.path.join(paths["source"], f))
            ]

            for filepath in files:
                process_file(filepath, channel, rmq_config, paths)

            time.sleep(3)
            sys.exit(0)

    except KeyboardInterrupt:
        logging.error("App stopped unexpectedly")

    finally:
        connection.close()
        logging.info("RabbitMQ connection closed")


if __name__ == "__main__":
    main()