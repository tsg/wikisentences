import psycopg2
import os
import json
import sys

PG_URL = os.environ.get("PG_URL")
BATCH_SIZE = 500  # Define the batch size you want


# expect the source jsonl file as the first argument
if len(sys.argv) != 2:
    print("Usage: python namespaces_to_pgvectory.py <inputfile>")
    exit(1)
inputfile = sys.argv[1]

def insert_batch_to_postgres(conn, batch):
    with conn.cursor() as cur:
        insert_query = "INSERT INTO docs (id, namespace, doc_id, source_id, vector_embedding) VALUES "
        formatted_data = [(
            row["id"], 
            row["namespace"],
            row["metadata"]["doc_id"],
            row["metadata"]["source_id"],
            row["vector"]) for row in batch]
        args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)", i).decode('utf-8') for i in formatted_data)

        cur.execute(insert_query + (args))
        conn.commit()

def main(conn):
    batch = []
    total = 0
    i = 0
    skip = 190000
    with open(inputfile, "r") as f:
        for line in f:
            if i < skip:
                i += 1
                continue
            obj = json.loads(line)
            batch.append(obj)
            if len(batch) >= BATCH_SIZE:
                insert_batch_to_postgres(conn, batch)
                total += len(batch)
                batch = []
                print(f"Inserted {skip + total} records")
        insert_batch_to_postgres(conn, batch)
        total += len(batch)
        print(f"Inserted {skip + total} records")

if __name__ == "__main__":
    with psycopg2.connect(PG_URL) as conn:
        main(conn)