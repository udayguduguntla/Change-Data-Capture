"""`main.py verify` queries run on a real engine graph and report what is there."""
from src.verify import verify
from tests.engine import Engine
from tests.test_integrity_engine import Session

ORG = 1000


def test_verify_reports_products_questions_and_checks():
    E = Engine()
    E.upsert("cdp.customer_profile", ts=1, cdp_id="C1", organization_id=ORG)
    E.upsert("ticketing.tickets", ts=1, id=10, organization_id=ORG, customer_cdp_id="C1",
             status="open", subject="s", priority="high", assignee_id=5)
    lines, summary = verify(Session(E), E.eff)
    text = "\n".join(lines)
    assert summary["wrong_product_labels"] == 0
    assert "| Support | " in text and "## Product questions on real data" in text
    assert "Which tickets has this customer raised? | 1/1" in text
    assert summary["questions"] == 38
