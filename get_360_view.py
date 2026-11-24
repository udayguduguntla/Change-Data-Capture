#!/usr/bin/env python3
"""
Standalone Contact 360 View Extractor for Neo4j.

This script fetches and displays a complete 360-degree graph view of a contact
for a given organization and identity. It is completely standalone and has no
dependencies on internal pipeline modules (such as src/ or config/ YAMLs).

Supported Identities:
    - Email address (e.g. carol@marvel.example)
    - Phone number with or without '+' (e.g. +442071404526 or 442071404526)
    - CDP Contact ID (e.g. CDP-csv-org1000-C010)
    - Campaign Contact ID (e.g. 529162318507960423)
    - Any ContactIdentity key value (e.g. customer_id, whatsapp, visitor_id)
    - SalesProfile ID or account_contact_id

Requirements:
    pip install neo4j python-dotenv

Usage:
    python get_360_view.py <organization_id> <identity>
    python get_360_view.py 1000 carol@marvel.example
    python get_360_view.py 1000 +442071404526
    python get_360_view.py 1000 CDP-csv-org1000-C010 --json
    python get_360_view.py 1000 carol@marvel.example --uri bolt://localhost:7687 --user neo4j --password secret
"""

import argparse
import json
import os
import sys
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple, Union

# Attempt to load .env if python-dotenv is installed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Ensure neo4j driver is installed
try:
    from neo4j import GraphDatabase
except ImportError:
    print(
        "Error: The 'neo4j' Python driver is required.\n"
        "Please install it using: pip install neo4j",
        file=sys.stderr,
    )
    sys.exit(1)


CYPHER_360_QUERY = """
// 1. Direct matches on Contact
OPTIONAL MATCH (c:Contact)
WHERE (c.organization_id = $org_id OR c.organization_id = toString($org_id))
  AND (c.cdp_id IN $identities OR toString(c.campaign_contact_id) IN $identities)
WITH collect(c) AS direct_matches

// 2. Identity matches via ContactIdentity (Email, Phone, WhatsApp, Customer ID, etc.)
OPTIONAL MATCH (id:ContactIdentity)
WHERE (id.organization_id = $org_id OR id.organization_id = toString($org_id))
  AND (
    id.identity_value IN $identities
    OR toLower(id.identity_value) IN $identities
    OR id.cdp_id IN $identities
    OR id.uid IN $identities
  )
OPTIONAL MATCH (id)<-[:HAS]-(:ContactProfile)<-[:HAS]-(c2:Contact)
WITH direct_matches, collect(c2) AS identity_matches

// 3. SalesProfile direct match (CRM Contact Profile ID or Account Contact ID)
OPTIONAL MATCH (sp:SalesProfile)
WHERE (sp.organization_id = $org_id OR sp.organization_id = toString($org_id))
  AND (sp.id IN $identities OR sp.account_contact_id IN $identities)
OPTIONAL MATCH (c3:Contact)-[:HAS]->(sp)
WITH direct_matches, identity_matches, collect(c3) AS sales_matches

// Combine all resolved Contact nodes
WITH [x IN direct_matches WHERE x IS NOT NULL]
   + [x IN identity_matches WHERE x IS NOT NULL]
   + [x IN sales_matches WHERE x IS NOT NULL] AS all_matches
UNWIND all_matches AS c
WITH DISTINCT c AS contact
WHERE contact IS NOT NULL

// 4. Direct relationships (distance 1, excluding 'HAS' collections)
OPTIONAL MATCH (contact)-[r]->(n)
WHERE type(r) <> 'HAS'
WITH contact, collect(DISTINCT {type: type(r), label: labels(n)[0], node: properties(n)}) AS direct_relations

// 5. Collection items (distance 2)
OPTIONAL MATCH (contact)-[:HAS]->(col)-[:HAS]->(item)
WITH contact, direct_relations, collect(DISTINCT {collection: labels(col)[0], item: properties(item)}) AS direct_items

// 6. Collection items (distance 3, period bucketed)
OPTIONAL MATCH (contact)-[:HAS]->(col2)-[:HAS_PERIOD]->(period)-[:HAS]->(item2)
WITH contact, direct_relations, direct_items, collect(DISTINCT {collection: labels(col2)[0], item: properties(item2)}) AS bucket_items

RETURN 
    properties(contact) AS contact_info,
    labels(contact) AS contact_labels,
    direct_relations,
    direct_items + bucket_items AS collections
"""


def _json_serializer(obj: Any) -> Any:
    """Helper to serialize Neo4j DateTime, Date, and other non-standard types to JSON."""
    if hasattr(obj, "iso_format"):
        return obj.iso_format()
    if hasattr(obj, "to_native"):
        return str(obj.to_native())
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def _format_val(val: Any) -> Any:
    """Format property values for clean terminal output (abbreviates large vectors/embeddings)."""
    if isinstance(val, (list, tuple)):
        if len(val) > 10:
            if all(isinstance(x, (int, float)) for x in val[:5]):
                return f"[<{len(val)} numerical dimensions / embedding>]"
            return f"[<{len(val)} items>]"
    return val


def resolve_neo4j_credentials(
    neo4j_config: Optional[dict] = None,
    uri: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    org_id: Optional[Union[int, str]] = None,
) -> Tuple[str, str, str, str]:
    """Resolve Neo4j credentials from arguments, config dictionary, or environment variables."""
    resolved_uri = uri
    resolved_user = user
    resolved_pass = password
    resolved_db = database

    # Backward compatibility with pipeline neo4j_config dict
    if neo4j_config and isinstance(neo4j_config, dict):
        if "instances" in neo4j_config and isinstance(neo4j_config["instances"], dict):
            default_inst = neo4j_config.get("default_instance") or next(iter(neo4j_config["instances"]))
            inst_cfg = neo4j_config["instances"].get(default_inst, {})
            resolved_uri = resolved_uri or inst_cfg.get("uri")
            resolved_user = resolved_user or inst_cfg.get("user")
            resolved_pass = resolved_pass or inst_cfg.get("password")
        else:
            resolved_uri = resolved_uri or neo4j_config.get("uri")
            resolved_user = resolved_user or neo4j_config.get("user")
            resolved_pass = resolved_pass or neo4j_config.get("password")
            resolved_db = resolved_db or neo4j_config.get("database")

    resolved_uri = resolved_uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
    resolved_user = resolved_user or os.getenv("NEO4J_USER", "neo4j")
    resolved_pass = resolved_pass or os.getenv("NEO4J_PASSWORD", "password")

    if not resolved_db:
        resolved_db = os.getenv("NEO4J_DATABASE")
        if not resolved_db and org_id:
            resolved_db = f"org{org_id}"
        if not resolved_db:
            resolved_db = "neo4j"

    return resolved_uri, resolved_user, resolved_pass, resolved_db


def print_360_view(records: List[Dict[str, Any]]) -> None:
    """Pretty-print contact 360-degree graph information to stdout."""
    for record in records:
        contact_info = record.get("contact_info", {})
        labels = record.get("contact_labels", [])
        direct_relations = record.get("direct_relations", [])
        collections = record.get("collections", [])

        # Organize collections by type
        organized_collections: Dict[str, List[Dict[str, Any]]] = {}
        for entry in collections:
            col_name = entry.get("collection")
            item = entry.get("item")
            if not col_name or not item:
                continue
            if col_name not in organized_collections:
                organized_collections[col_name] = []
            organized_collections[col_name].append(dict(item))

        print("=" * 80)
        print(f"CONTACT LABELS: {labels}")
        print("CONTACT PROPERTIES:")
        for k, v in contact_info.items():
            print(f"  {k}: {_format_val(v)}")

        print("\nDIRECT RELATIONSHIPS:")
        for rel in direct_relations:
            if not rel.get("type"):
                continue
            print(f"  - [{rel['type']}] -> {rel.get('label')}")
            node_props = rel.get("node", {}) or {}
            for k, v in list(node_props.items())[:3]:
                print(f"      {k}: {_format_val(v)}")
            if len(node_props) > 3:
                print(f"      ... and {len(node_props) - 3} more properties.")

        print("\nCOLLECTIONS:")
        for col_name, items in organized_collections.items():
            print(f"  - {col_name} ({len(items)} items)")
            for i, it in enumerate(items[:3]):  # preview first 3 items
                clean_preview = {k: _format_val(v) for k, v in it.items()}
                print(f"    {i+1}: {clean_preview}")
            if len(items) > 3:
                print(f"    ... and {len(items) - 3} more.")
        print("=" * 80)


def fetch_contact_360(
    organization_id: Union[int, str],
    identity: str,
    neo4j_config: Optional[dict] = None,
    uri: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    output_json: bool = False,
) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch a complete 360-degree view of a contact from Neo4j.

    Parameters:
        organization_id: The tenant/organization identifier (e.g., 1000).
        identity: Value to match against:
                  - Email address (e.g. user@example.com)
                  - Phone number with/without '+' (e.g. +442071404526 or 442071404526)
                  - CDP ID (e.g. CDP-csv-org1000-C010)
                  - Campaign Contact ID
                  - Any ContactIdentity value or SalesProfile ID.
        neo4j_config: (Optional) Config dictionary for backward compatibility.
        uri: (Optional) Neo4j bolt URI.
        user: (Optional) Neo4j username.
        password: (Optional) Neo4j password.
        database: (Optional) Target Neo4j database (defaults to org<org_id> with fallback to neo4j).
        output_json: If True, prints formatted JSON to stdout instead of terminal summary.

    Returns:
        List of matching contact record dictionaries or None if not found.
    """
    resolved_uri, resolved_user, resolved_pass, resolved_db = resolve_neo4j_credentials(
        neo4j_config=neo4j_config,
        uri=uri,
        user=user,
        password=password,
        database=database,
        org_id=organization_id,
    )

    driver = GraphDatabase.driver(resolved_uri, auth=(resolved_user, resolved_pass))
    records: List[Dict[str, Any]] = []

    # Prepare identity search variants (phone with/without +, case-insensitive email, etc.)
    ident_str = str(identity).strip()
    variants = list({
        ident_str,
        ident_str.lower(),
        ident_str.lstrip("+"),
        f"+{ident_str.lstrip('+')}",
    })

    try:
        org_param = int(organization_id) if str(organization_id).isdigit() else organization_id
        # Attempt query on the tenant database; if it fails (e.g. single-DB setup), fallback to 'neo4j'
        try:
            with driver.session(database=resolved_db) as session:
                result = session.run(
                    CYPHER_360_QUERY,
                    org_id=org_param,
                    identities=variants,
                )
                records = [record.data() for record in result]
        except Exception as primary_err:
            if resolved_db != "neo4j":
                try:
                    with driver.session(database="neo4j") as session:
                        result = session.run(
                            CYPHER_360_QUERY,
                            org_id=org_param,
                            identities=variants,
                        )
                        records = [record.data() for record in result]
                except Exception:
                    raise primary_err
            else:
                raise primary_err
    finally:
        driver.close()

    if not records:
        print(f"No contact found for identity '{identity}' in organization {organization_id}")
        return None

    if output_json:
        print(json.dumps(records, indent=2, default=_json_serializer))
    else:
        print_360_view(records)

    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch and display a complete 360-degree view of a Contact in Neo4j (Standalone)."
    )
    parser.add_argument("organization_id", help="Organization ID (e.g., 1000)")
    parser.add_argument(
        "identity",
        help="Contact identity: Email, Phone number (+ or plain), CDP ID, or Campaign Contact ID",
    )
    parser.add_argument(
        "--uri",
        "-u",
        default=None,
        help="Neo4j Bolt URI (default: env NEO4J_URI or bolt://localhost:7687)",
    )
    parser.add_argument(
        "--user",
        default=None,
        help="Neo4j username (default: env NEO4J_USER or neo4j)",
    )
    parser.add_argument(
        "--password",
        "-p",
        default=None,
        help="Neo4j password (default: env NEO4J_PASSWORD or password)",
    )
    parser.add_argument(
        "--database",
        "-d",
        default=None,
        help="Neo4j database name (default: env NEO4J_DATABASE or org<organization_id>)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Path to an optional custom .env file",
    )

    args = parser.parse_args()

    if args.env_file:
        try:
            from dotenv import load_dotenv
            load_dotenv(args.env_file, override=True)
        except ImportError:
            pass

    fetch_contact_360(
        organization_id=args.organization_id,
        identity=args.identity,
        uri=args.uri,
        user=args.user,
        password=args.password,
        database=args.database,
        output_json=args.json,
    )
