import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer

from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH_GREEN, INPUT_DATA_PATH_FHV,\
    PRODUCE_TOPIC_GREEN_RIDES, PRODUCE_TOPIC_FHV_RIDES, CONSUME_TOPIC_GREEN_RIDES, CONSUME_TOPIC_FHV_RIDES

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # vendor_id, PULocationID
                records.append(f'{row[0]}, {row[5]}')
                ride_keys.append(str(row[0]))
                i += 1
                if i == 100000:
                    break
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                #print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }
    producer = RideCSVProducer(props=config)
    ride_records_green = producer.read_records(resource_path=INPUT_DATA_PATH_GREEN)
    ride_records_fhv = producer.read_records(resource_path=INPUT_DATA_PATH_FHV)
    #print(ride_records)
    producer.publish(topic=CONSUME_TOPIC_GREEN_RIDES, records=ride_records_green)
    producer.publish(topic=CONSUME_TOPIC_FHV_RIDES, records=ride_records_fhv)
