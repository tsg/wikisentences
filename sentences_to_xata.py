
from sentence_transformers import SentenceTransformer
from xata.client import XataClient
from xata.helpers import BulkProcessor

client = XataClient()
bp = BulkProcessor(client)

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

def compute_embeddings(sentences, model_name="sentence-transformers/all-MiniLM-L6-v2"):
    model = SentenceTransformer(model_name)
    return model.encode(sentences)

def main():
    filename = "/Users/tsg/Downloads/wikisent2.txt"
    batch_size = 500  # Adjust as needed
    i = 0
    skip = 150
    for batch in read_file_in_batches(filename, batch_size):
        if i < skip:
            i += 1
            continue
        embeddings = compute_embeddings(batch)
        data = [{"sentence": sentence, "vector_embedding": embedding.tolist() * 4} # multiply with 4 to have the dimension 1536
                    for sentence, embedding in zip(batch, embeddings)]
        bp.put_records("docs", data)
        bp.flush_queue()
        i += 1
        print("Wrote batch", i, " (", i*batch_size, " records)")

if __name__ == "__main__":
    main()