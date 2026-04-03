import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ESConfig:
    """Holds all connection and behaviour settings for the Elasticsearch client.

    Values are read from environment variables (via .env). Only ES_HOST and
    ES_API_KEY are required; the scroll settings have sensible defaults.
    """
    host: str
    api_key: str
    index: str
    scroll_size: int = 5000
    scroll_timeout: str = "2m"


def load_config() -> ESConfig:
    """Reads environment variables and returns a populated ESConfig.

    Required env vars: ES_HOST, ES_API_KEY.
    Optional env vars: ES_SCROLL_SIZE (default 5000), ES_SCROLL_TIMEOUT (default '2m').

    Raises:
        ValueError: if ES_HOST or ES_API_KEY are missing.
    """
    host = os.getenv("ES_HOST")
    api_key = os.getenv("ES_API_KEY")
    index=os.getenv("ES_INDEX")

    missing = [name for name, val in [("ES_HOST", host), ("ES_API_KEY", api_key),  ("ES_INDEX", index)] if not val]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return ESConfig(
        host=host,
        api_key=api_key,
        index=index,
        scroll_size=int(os.getenv("ES_SCROLL_SIZE", 5000)),
        scroll_timeout=os.getenv("ES_SCROLL_TIMEOUT", "2m"),
    )
