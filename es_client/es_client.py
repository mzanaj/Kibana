import io
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from config import ESConfig


class ESClient:
    """Standard reusable Elasticsearch client.

    Handles connection verification and data extraction with transparent
    scrolling — works on any index size without any extra configuration.

    Usage:
        from config import load_config
        from es_client import ESClient

        client = ESClient(load_config())
        df = client.pull("my-index")
    """

    def __init__(self, config: ESConfig):
        """Connects to Elasticsearch and verifies the connection with a ping.

        Args:
            config: ESConfig instance with host, api_key, and scroll settings.

        Raises:
            ConnectionError: if the cluster is unreachable.
        """
        print(f"[ESClient] Connecting to {config.host}...")

        self.config = config
        self.es = Elasticsearch(
            config.host,
            api_key=config.api_key,
        )

        if not self.es.ping():
            raise ConnectionError(
                f"[ESClient] Could not reach Elasticsearch at {config.host}. "
                "Check your ES_HOST and ES_API_KEY in .env."
            )

        print("[ESClient] Connection successful.")

    def pull(self, index: str=self.config.index, fields: list = None, query: dict = None, fmt: str = "df"):
        """Pulls documents from an Elasticsearch index, scrolling transparently.

        Fetches every document that matches the query (default: all documents).
        Uses the Scroll API under the hood, so index size is never a constraint.

        Args:
            index:  Name of the index (or index pattern, e.g. 'logs-*').
            fields: List of field names to return. Default None returns all fields.
            query:  Raw Elasticsearch query dict. Default None uses match_all.
            fmt:    Return format — 'df' (DataFrame, default), 'json' (list of
                    dicts), or 'parquet' (bytes buffer, ready to write to disk).

        Returns:
            pd.DataFrame | list[dict] | io.BytesIO depending on fmt.

        Raises:
            ValueError: if fmt is not one of 'df', 'json', 'parquet'.
        """
        if fmt not in ("df", "json", "parquet"):
            raise ValueError(f"[ESClient] fmt must be 'df', 'json', or 'parquet'. Got: '{fmt}'")

        query = query or {"match_all": {}}
        source = fields if fields else True

        print(f"[ESClient] Pulling index: '{index}'...")
        print(f"[ESClient] Query: {query}")
        print(f"[ESClient] Fields: {fields if fields else 'all'}")
        print("[ESClient] Scrolling...")

        docs = []
        for hit in scan(
            self.es,
            index=index,
            query={"query": query},
            source=source,
            size=self.config.scroll_size,
            scroll=self.config.scroll_timeout,
        ):
            docs.append(hit["_source"])
            if len(docs) % self.config.scroll_size == 0:
                print(f"[ESClient]   {len(docs):,} docs fetched...")

        print(f"[ESClient] Done. {len(docs):,} documents retrieved.")

        if fmt == "json":
            print("[ESClient] Returning as JSON (list of dicts).")
            return docs

        df = pd.DataFrame(docs)

        if fmt == "parquet":
            print("[ESClient] Returning as Parquet (bytes buffer).")
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            return buffer

        print("[ESClient] Returning as DataFrame.")
        return df
