import shutil
from pathlib import Path

import pytest

from src.config_loader import ConfigLoader
from src.errors import ConfigError
from src.validation import validate_all

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def cfg(tmp_path):
    """A writable copy of the real config, so tests can corrupt it."""
    dst = tmp_path / "config"
    shutil.copytree(ROOT / "config", dst)
    return dst


def errors_for(cfg) -> list[str]:
    return validate_all(ConfigLoader(cfg)).errors


def write_overlay(cfg, name: str, body: str) -> None:
    (cfg / "mappings" / f"{name}.yaml").write_text(body)


def test_shipped_config_is_valid(cfg):
    """The config we ship must pass the validator we ship."""
    report = validate_all(ConfigLoader(cfg))
    assert report.errors == []


def test_template_is_excluded_from_validation(cfg):
    assert (cfg / "mappings" / "_TEMPLATE.yaml").exists()
    assert validate_all(ConfigLoader(cfg)).errors == []


def test_extends_unknown_table(cfg):
    write_overlay(cfg, "orgtest01", """
database: orgtest01
organization_id: "test01"
products: [crm]
extends:
  crm.nonexistent:
    extra_labels: [X]
""")
    assert any("not declared in core.yaml" in e for e in errors_for(cfg))


def test_filename_must_match_the_database_field(cfg):
    write_overlay(cfg, "orgtest13", """
database: orgdifferent
organization_id: "test13"
products: [crm]
""")
    assert any("these must match" in e for e in errors_for(cfg))


def test_unquoted_numeric_organisation_id_rejected(cfg):
    """YAML coerces bare 12345 to an integer, which silently breaks string lookups."""
    write_overlay(cfg, "orgtest14", """
database: orgtest14
organization_id: 12345
products: [crm]
""")
    errs = errors_for(cfg)
    assert any("Quote it" in e and "int" in e for e in errs)


def test_unquoted_yes_organisation_id_becomes_a_boolean(cfg):
    write_overlay(cfg, "orgtest15", """
database: orgtest15
organization_id: yes
products: [crm]
""")
    assert any("bool" in e for e in errors_for(cfg))


def test_filename_with_an_underscore_rejected(cfg):
    write_overlay(cfg, "org_test16", """
database: org_test16
organization_id: "test16"
products: [crm]
""")
    assert any("not a legal Neo4j database name" in e for e in errors_for(cfg))


def test_missing_raw_file(cfg):
    write_overlay(cfg, "orgtest17", """
database: orgtest17
organization_id: "test17"
products: [crm]
raw:
  crm.contacts: mappings/raw/absent.cypher
""")
    assert any("raw Cypher file not found" in e for e in errors_for(cfg))


def test_raw_file_without_batch_token(cfg):
    (cfg / "mappings" / "raw" / "bad.cypher").write_text("MATCH (n) RETURN n")
    write_overlay(cfg, "orgtest18", """
database: orgtest18
organization_id: "test18"
products: [crm]
raw:
  crm.contacts: mappings/raw/bad.cypher
""")
    assert any("$batch" in e for e in errors_for(cfg))


def test_valid_raw_file_accepted(cfg):
    (cfg / "mappings" / "raw" / "ok.cypher").write_text(
        "UNWIND $batch AS e MERGE (n:Contact {contact_id: e.key})")
    write_overlay(cfg, "orgtest19", """
database: orgtest19
organization_id: "test19"
products: [crm]
raw:
  crm.contacts: mappings/raw/ok.cypher
""")
    assert errors_for(cfg) == []


# --- merge semantics -------------------------------------------------------


def test_missing_env_var_fails_only_when_that_section_is_read(tmp_path, monkeypatch):
    """
    ${VAR} is expanded per section, not at load time. An offline command must not demand a database password it will never use.
    """
    shutil.copytree(ROOT / "config", tmp_path / "config")
    monkeypatch.delenv("PG_PASSWORD", raising=False)
    loader = ConfigLoader(tmp_path / "config")          # constructing is fine
    with pytest.raises(ConfigError, match="PG_PASSWORD"):
        loader.section("postgres")                      # reading it is not


