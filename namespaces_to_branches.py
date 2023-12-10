from xata.client import XataClient
from xata.helpers import BulkProcessor
import time
import json

client = XataClient()

def main():
    namespaces = {}
    with open("/Users/tsg/tmp/fixies/pinecone_vectors.jsonl", "r") as f:
        for line in f:
            obj = json.loads(line)
            namespace = obj["namespace"]
            if namespace not in namespaces:
                namespaces[namespace] = 1
                print("new namespace", namespace)
            else:
                namespaces[namespace] += 1
    print(namespaces)

if __name__ == "__main__":
    main()