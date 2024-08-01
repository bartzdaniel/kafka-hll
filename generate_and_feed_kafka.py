import random
import time
from kafka import KafkaProducer
import os
import sys

def generate_entries(num_entries):
    for _ in range(num_entries):
        timestamp = int(time.time())
        data_subject_id = random.randint(1000000, 2000000)
        viewer_id = random.randint(1, 100)
        entry = f"{timestamp}:{data_subject_id}:{viewer_id}"
        yield entry

def send_to_kafka(entries, num_entries):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9094',
        acks='all',  # Ensure all replicas acknowledge
        retries=5,  # Retry up to 5 times
        max_in_flight_requests_per_connection=5,  # Ensure max in-flight requests are 5 or less
        batch_size=16384,
        linger_ms=10
    )
    
    # Read the last sent message line number
    last_sent = 0
    if os.path.exists('progress.log'):
        with open('progress.log', 'r') as progress_file:
            last_sent = int(progress_file.read().strip())

    for i, entry in enumerate(entries, start=1):
        if i > last_sent:
            producer.send('data-topic', entry.encode('utf-8'))
            # Update the progress file every 100,000 messages
            if i % 100000 == 0:
                with open('progress.log', 'w') as progress_file:
                    progress_file.write(str(i))
                print(f"Sent {i} messages")

    producer.flush()
    # Final write to ensure progress is saved
    with open('progress.log', 'w') as progress_file:
        progress_file.write(str(num_entries))
    print("Finished sending messages")

if __name__ == "__main__":
    num_entries = 10000000  # Default value

    if len(sys.argv) == 2:
        num_entries = int(sys.argv[1])

    entries = generate_entries(num_entries)
    send_to_kafka(entries, num_entries)

