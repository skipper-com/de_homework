from confluent_kafka import Producer
from confluent_kafka import Consumer
from configparser import ConfigParser


config_parser = ConfigParser()
config_parser.read_file(open('/home/skipper/data-engineering-zoomcamp/homework/6_week/client.properties'))
config = dict(config_parser['default'])

producer = Producer(config)
topic = "zoomcamp_topic"
value = """{
    'ordertime': 1504640010123,
    'orderid': 45000,
    'itemid': "XXXXXXXXX",
    'orderunits': 3,
    'address': {
        'city': 'YYYYY',
        'state': 'ZZZZZ',
        'zipcode': 99999
    }
}"""
key = "45000"
producer.produce(topic=topic, value=value, key=key)

# props["group.id"] = "python-group-1"
# props["auto.offset.reset"] = "earliest"

# consumer = Consumer(props)
# consumer.subscribe(["zoomcamp_topic"])
# try:
#     while True:
#         msg = consumer.poll(1.0)
#         if msg is not None and msg.error() is None:
#             print("key = {key:12} value = {value:12}".format(key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
# except KeyboardInterrupt:
#     pass
# finally:
#     consumer.close()