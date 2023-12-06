from sentence_transformers import SentenceTransformer
from xata.client import XataClient
import sys
import time
import json

def main():
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    vector = model.encode(sys.argv[1])

    print(json.dumps(vector.tolist()))
    return

    xata = XataClient()

    start = time.time()
    results = xata.data().vector_search("sentences", {
        "queryVector": vector.tolist(),
        "column": "embedding",
        "size": 5
    })
    print(f"Search took {time.time() - start} seconds")
    for result in results["records"]:
        print(f'{result["sentence"]}\t{result["xata"]["score"]}\t{result["xata"]}')

if __name__ == "__main__":
    main()