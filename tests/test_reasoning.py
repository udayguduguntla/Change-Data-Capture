"""Product questions as graph paths (scripts/reasoning_check.py): each must stay answerable,
and each boundary question must stay blocked."""
import importlib.util
from pathlib import Path

import pytest

from src.config_loader import ConfigLoader

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("reasoning_check", ROOT / "scripts" / "reasoning_check.py")
rc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rc)
EFF = ConfigLoader(ROOT / "config").effective("org1000")


@pytest.mark.parametrize("agent,products,question,paths,expect", rc.QUESTIONS,
                         ids=[q[2][:60] for q in rc.QUESTIONS])
def test_question(agent, products, question, paths, expect):
    got, notes = rc.check(EFF, products, paths)
    assert got == expect, notes


def test_the_check_catches_a_wrong_direction_and_a_missing_edge():
    assert rc.check(EFF, rc.HUB, [[("SupportAgent", "ASSIGNED_TO", "Ticket")]])[0] == "missing"
    assert rc.check(EFF, rc.HUB, [[("Ticket", "SOLD_TO", "Contact")]])[0] == "missing"
