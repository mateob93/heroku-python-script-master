# MQTT suscriber
# Continuosly monitor a MQTT topic for data

from mqtt_client import MqttClient
import config


def main():
    client = MqttClient(config.HOST, config.USER, config.PW)


if __name__ == "__main__":
    main()
