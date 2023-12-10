import psycopg2
import os
import sys 
import numpy
import json
import time
PG_URL = os.environ.get("PG_URL")

# expect the namespace name as the first argument
if len(sys.argv) != 2:
    print("Usage: python random_search_pgvector.py <namespace>")
    exit(1)
namespace = sys.argv[1]

def random_embedding():
    return numpy.random.uniform(low=-2.1, high=4.0, size=(1536,))

def main(conn):
    embedding = random_embedding().tolist()
    with conn.cursor() as cur:
        start = time.time()

        cur.execute(
            "SELECT id, vector_embedding <=> %s as distance FROM docs WHERE namespace = %s ORDER BY distance LIMIT 5", 
            (json.dumps(embedding),namespace))
        results = cur.fetchall()
        print(f"Search took {time.time() - start} seconds")
        for result in results:
            print(f'{result[0]}\t{result[1]}')

if __name__ == "__main__":
    with psycopg2.connect(PG_URL) as conn:
        main(conn)