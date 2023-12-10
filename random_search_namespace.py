import numpy 
import time
from xata.client import XataClient
import sys

# expect the namespace name as the first argument
if len(sys.argv) != 2:
    print("Usage: python random_search_namespace.py <namespace>")
    exit(1)
namespace = sys.argv[1]

def random_embedding():
    return numpy.random.uniform(low=-2.1, high=4.0, size=(1536,))


def main():
    xata = XataClient()

    start = time.time()
    results = xata.data().vector_search("docs", {
        "queryVector": random_embedding().tolist(),
        "column": "vector_embedding",
        "size": 5
    }, branch_name=namespace)
    print(f"Search took {time.time() - start} seconds")
    for result in results["records"]:
        print(f'{result["id"]}\t{result["xata"]["score"]}')

if __name__ == "__main__":
    main()