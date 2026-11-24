import json
from typing import Dict, Any, Optional

def adapt_clickhouse_payload(kafka_msg_value: bytes, table_name: str) -> Optional[Dict[str, Any]]:
    """Adapter for ClickHouse CDC JSON payloads from the Kafka engine MV sink."""
    if not kafka_msg_value:
        return None

    try:
        raw = json.loads(kafka_msg_value.decode('utf-8'))
    except json.JSONDecodeError:
        return None

    # Poller payloads already have the envelope
    if "op" in raw and "after" in raw:
        return raw

    # MV payloads are flat. MVs only trigger on INSERT.
    return {
        "op": "c",
        "after": raw,
        "source": {
            "db": "webtracking", # Assumed default for now
            "table": table_name,
            "snapshot": "false"
        }
    }
