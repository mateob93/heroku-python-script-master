import paho.mqtt.client as mqtt
import upload_database
from topics import Topics


class MqttClient:
    # class variables
    amount_of_receptions = 0
    topics = [x.value for x in Topics]
    topics_and_messages = {}
    for topic in topics:
        topics_and_messages[topic] = ""

    # instance methods
    def __init__(self, host, usr, password):
        self.client = mqtt.Client(client_id="5ub5cr1b3r_cl13n7_1d", protocol=mqtt.MQTTv311, clean_session=False)
        self.client.on_connect = MqttClient.on_connect
        self.client.on_message = MqttClient.on_message
        self.client.on_disconnect = MqttClient.on_disconnect
        self.client.username_pw_set(usr, password)
        self.client.reconnect_delay_set(min_delay=25, max_delay=30)
        self.client.connect(host=host, port=18096, keepalive=60)

        self.subscribe_to_topics()

        self.client.loop_forever()

    def subscribe_to_topics(self):
        for topic in MqttClient.topics:
            (res, mid) = self.client.subscribe(topic, 1)
            print("Subscribed to " + topic + " with result code " + str(res))

    @staticmethod
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected with result code " + str(rc))
        else:
            print("Bad Connection. Result code: " + str(rc))

    @staticmethod
    def on_disconnect(client, userdata, rc):
        if rc != 0:
            print("Unexpected disconnection.")

    #            with open("errors.log", "a") as e:
    #                e.write("[" + str(datetime.datetime.now()) + "]: " "Unexpected disconnection.\n")

    # The callback for when a PUBLISH message is received from the server
    @staticmethod
    def on_message(client, userdata, msg):
        MqttClient.amount_of_receptions = MqttClient.amount_of_receptions + 1
        print("Message received: ")
        print(msg.topic, " ", msg.payload.decode())
        #        with open("result.txt", "a") as f:
        #            f.write(msg.payload.decode())
        print("Amount of receptions so far:", MqttClient.amount_of_receptions)
        if msg.payload.decode() == 'Finish':
            upload_database.upload_to_database(msg.topic, MqttClient.topics_and_messages[msg.topic])
        else:
            MqttClient.topics_and_messages[msg.topic] = MqttClient.topics_and_messages.get(msg.topic) + msg.payload.decode()
