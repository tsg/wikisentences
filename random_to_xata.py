
from xata.client import XataClient
from xata.helpers import BulkProcessor
import numpy 
import threading
import time


def read_file_in_batches(filename, batch_size):
    with open(filename, 'r') as file:
        batch = []
        for line in file:
            batch.append(line.strip())
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

def random_embedding():
    return numpy.random.uniform(low=-2.1, high=4.0, size=(1536,))

def report_records_per_minute(records_per_minute, lock):
    while True:
        time.sleep(60)  # Wait for one minute
        with lock:
            print(f"Records inserted in the last minute: {records_per_minute[0]}")
            records_per_minute[0] = 0  # Reset the count for the next minute


def worker(thread_id, batch_size, global_counter, records_per_minute, lock):
    client = XataClient()
    bp = BulkProcessor(client)

    print("Starting thread", thread_id)
    while True:
        data = [{
            "sentence": "This is a test",
            "vector_embedding": random_embedding().tolist()}
            for _ in range(batch_size)]
        
        bp.put_records("docs", data)
        bp.flush_queue()
        
        with lock:
            global_counter[0] += 1
            records_per_minute[0] += batch_size
            print(f"Wrote batch {global_counter[0]} ({global_counter[0]*batch_size} records)")

def main():
    threads = []
    batch_size = 1000  # Adjust as needed
    global_counter = [0]  # Using a list as a mutable object
    records_per_minute = [0]  # Records inserted per minute
    lock = threading.Lock()

    for i in range(10):  # Creating 10 threads
        t = threading.Thread(target=worker, args=(i, batch_size, global_counter, records_per_minute, lock))
        t.start()
        threads.append(t)

    # Start a separate thread for reporting records per minute
    reporter_thread = threading.Thread(target=report_records_per_minute, args=(records_per_minute, lock))
    reporter_thread.start()
    threads.append(reporter_thread)

    for t in threads:
        t.join()  # Waiting for all threads to complete

if __name__ == "__main__":
    main()