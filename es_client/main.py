from config import load_config
from es_client import ESClient

config = load_config()
client = ESClient(config)

# Pull full index as DataFrame (default)
df = client.pull() # Defaults to client.pull(config.index)

print(df.head())

# Pull specific fields only
df_filtered = client.pull("my-index", fields=["timestamp", "value", "status"])
print(df_filtered.head())

# Pull with a custom query, return as JSON
data = client.pull(
    "my-index",
    query={"term": {"status": "active"}},
    fmt="json",
)
print(data[:2])

# Pull and return as Parquet buffer (e.g. to write to disk or send to S3)
buffer = client.pull("my-index", fmt="parquet")
with open("output.parquet", "wb") as f:
    f.write(buffer.read())
