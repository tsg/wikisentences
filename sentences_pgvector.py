import csv
import gzip
import psycopg2
import os
import json

PG_URL = os.environ.get("PG_URL")

BATCH_SIZE = 1000  # Define the batch size you want


def insert_batch_to_postgres(conn, batch):
    with conn.cursor() as cur:
        insert_query = "INSERT INTO sentences (sentence, embedding) VALUES "
        # Convert each row's embedding from string to list and prepare for insertion
        formatted_data = [(row[0], row[1]) for row in batch]
        args = ','.join(cur.mogrify("(%s,%s)", i).decode('utf-8') for i in formatted_data)

        cur.execute(insert_query + (args))
        conn.commit()


def process_csv_gz(file_path):
    with psycopg2.connect(PG_URL) as conn:
        with gzip.open(file_path, 'rt') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header

            batch = []
            skip = 0
            i = 0
            for row in reader:
                if i < skip:
                    i += 1
                    continue
                batch.append(row)
                i+=1
                if len(batch) == BATCH_SIZE:
                    insert_batch_to_postgres(conn, batch)
                    batch = []
                    print("Inserted ", i, " records")

            if batch:
                insert_batch_to_postgres(conn, batch)
            print("Inserted ", i, " records")



if __name__ == '__main__':
    process_csv_gz('sentences.2.csv.gz')