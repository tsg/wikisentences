
from xata.client import XataClient
from xata.helpers import BulkProcessor
import numpy 
import threading


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
    return numpy.random.uniform(low=0.5, high=13.3, size=(1536,))

def worker(thread_id, batch_size, global_counter, lock):
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
            print(f"Wrote batch {global_counter[0]} ({global_counter[0]*batch_size} records)")

def main():
    threads = []
    batch_size = 1000  # Adjust as needed
    global_counter = [0]  # Using a list as a mutable object
    lock = threading.Lock()

    for i in range(10):  # Creating 10 threads
        t = threading.Thread(target=worker, args=(i, batch_size, global_counter, lock))
        t.start()
        threads.append(t)
        
    for t in threads:
        t.join()  # Waiting for all threads to complete

if __name__ == "__main__":
    main()