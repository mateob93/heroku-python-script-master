# MQTT suscriber
# Continuosly monitor a MQTT topic for data

from mqtt_client import MqttClient


def main():
    client = MqttClient("tailor.cloudmqtt.com", "sfluras-dev", "sebas32")


if __name__ == "__main__":
    main()
