# es_client

A minimal, reusable Elasticsearch client for Python. Connect, pull data from any index (any size), and get it back as a DataFrame, JSON, or Parquet.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your credentials
```

**.env**
```
ES_HOST=http://localhost:9200
ES_API_KEY=your_api_key_here
ES_INDEX=my_index
```

## Usage

```python
from config import load_config
from es_client import ESClient

client = ESClient(load_config())

df     = client.pull()                                   # full index → DataFrame
data   = client.pull(fmt="json")                         # full index → list of dicts
buf    = client.pull(fmt="parquet")                      # full index → BytesIO buffer

df     = client.pull(fields=["timestamp", "value"])      # specific fields only
df     = client.pull(query={"term": {"status": "active"}})  # custom query
```

`pull()` scrolls transparently — no matter how large the index, you get everything.

## Auth

Uses **API key** auth (ES 8+). Generate one from Kibana under Stack Management → API Keys, then paste it into `.env`. The `.env` file is gitignored — credentials never touch version control.
