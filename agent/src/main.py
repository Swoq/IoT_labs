from paho.mqtt import client as mqtt_client
import time

from schema.aggregated_accelerometer_schema import AggregatedAccelerometerSchema
from schema.aggregated_parking_schema import AggregatedParkingSchema
from file_datasource import FileDatasource
import config


def connect_mqtt(broker, port):
    """Create MQTT client"""
    print(f"CONNECT TO {broker}:{port}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            print("Failed to connect {broker}:{port}, return code %d\n", rc)
            exit(rc)  # Stop execution

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client


def publish(client, datasource, delay, msg_functions):
    datasource.start_reading()
    while True:
        time.sleep(delay)
        for get_msg in msg_functions:
            topic, msg = get_msg(datasource)
            result = client.publish(topic, msg)
            result: [0, 1]
            status = result[0]
            if status == 0:
                pass
                print(f"Send `{msg}` to topic `{topic}`")
            else:
                print(f"Failed to send message to topic {topic}")


def get_mqtt_parking_msg(datasource):
    data = datasource.read_parking_data()
    msg = AggregatedParkingSchema().dumps(data)
    return [config.MQTT_PARKING_DATA_TOPIC, msg]


def get_mqtt_accelerometer_msg(datasource):
    data = datasource.read_accelerometer_data()
    msg = AggregatedAccelerometerSchema().dumps(data)
    return [config.MQTT_ACCELEROMETER_DATA_TOPIC, msg]


def run():
    # Prepare mqtt client
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    # Prepare datasource
    datasource = FileDatasource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")

    msg_publishers = [get_mqtt_accelerometer_msg, get_mqtt_parking_msg]

    # Infinity publish data
    publish(client, datasource, config.DELAY, msg_publishers)


if __name__ == "__main__":
    run()
