"""Tests for human-readable ontology (no wt:*, Contact-only tickets)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
KG = ROOT / "Knowledge_Graph_Pipeline"
sys.path.insert(0, str(KG))
sys.path.insert(0, str(ROOT))

from src.config_loader import ConfigLoader


def loader_derived():
    loader = ConfigLoader(config_dir=str(KG / "config"))
    return loader.core_meta["derived"]


def test_assigned_to_resolved_by_verbs():
    pass

    root = ROOT / "ontology"
    bad = ("ASSIGNED_L1", "RESOLVER_L2", "FOR_CUSTOMER")
    for rel_path in list((root / "modules").glob("*.yml")) + list(
        (root / "slices").glob("*.yml")
    ):
        text = rel_path.read_text(encoding="utf-8")
        for token in bad:
            assert token not in text, f"{rel_path.name} still has {token}"


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
