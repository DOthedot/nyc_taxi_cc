import json
import pyarrow.parquet as pq
import pyarrow as pa
from confluent_kafka import Producer
from typing import List, Dict

from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, KAFKA_TOPIC


class JsonProducer(Producer):
    def __init__(self, props: Dict):
        self.producer = Producer(**props)

    def publish_rides(self, topic: str, file_path: str):
            try:
                # Load Parquet file
                print("Loading parquet file...")
                yellow_taxi = pq.ParquetFile(file_path)

                index = 1
                total_records = 0

                for batch in yellow_taxi.iter_batches(batch_size=1000):
                    chunk_df = pa.Table.from_batches([batch]).to_pandas()
                    records = chunk_df.to_dict('records')
    
                    print(f"Sending Batch {index} ({len(records)} records)")
    
                    # Send to Kafka
                    for record in records:
                        self.producer.produce(
                            'yellow_taxi_bookings',
                            value=json.dumps(record, default=str).encode()
                        )
    
                    self.producer.flush()
                    total_records += len(records)
                    index += 1
               
            except Exception as e:
                print(e.__str__())


if __name__ == '__main__':
    # Config Should match with the KafkaProducer expectation
    # kafka expects binary format for the key-value pair

    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'green_taxi_producer',
        'acks': 'all',
        'enable.idempotence': True,
        'linger.ms': 5,
        'batch.size': 16384,
        'compression.type': 'snappy',
    }      

    
    producer = JsonProducer(props=config)
    
    producer.publish_rides(topic=KAFKA_TOPIC, file_path=INPUT_DATA_PATH)