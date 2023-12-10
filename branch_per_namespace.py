from xata.client import XataClient
from xata.helpers import BulkProcessor
import uuid
import numpy 
import time

client = XataClient()

def random_embedding():
    return numpy.random.uniform(low=-2.1, high=4.0, size=(1536,))

def main():
    batch_size = 1000
    for i in range(456, 1000):
        namespace = f"namespace{i:03d}"
        start = time.time()
        client = XataClient(branch_name=namespace)
        print("creating namespace", namespace)
        resp = client.branch().create({"from": "main"}, branch_name=namespace)
        if resp.status_code != 201:
            print("Error creating namespace", namespace)
            print(resp.json())
            return
    
        print("writing data to namespace", namespace)
        bp = BulkProcessor(client)
        data = [{
            "vector_id": uuid.uuid4().hex,
            "vector_embedding": random_embedding().tolist()
        } for _ in range(batch_size)]
        bp.put_records("docs", data)
        bp.flush_queue()
        print(f"Creating namespace with data took {time.time() - start} seconds")

if __name__ == "__main__":
    main()