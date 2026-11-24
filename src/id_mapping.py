"""Id projections used by the product apps.

Campaign stores a contact as a bigint that stands in for CDP's text `cdp_id`
(backend/src/helpers/id-schema.ts, campaignContactIdFromCdp):

  "12345"                           -> 12345                      (digits only)
  "CDP-campaign-org1012-1790413..." -> 1790413...                 (trailing 10+ digits)
  anything else (UUID, CDP-shop-...) -> first 15 hex chars of sha256("campaign-id:" + cdp_id), as int

The pipeline writes this value on every Contact as `campaign_contact_id`, so Campaign rows
find their contact by it. When Campaign stores the full cdp_id instead, point the three
campaign references in core.yaml back at `cdp_id` and drop this.
"""
from __future__ import annotations

import hashlib
import re

_TAIL = re.compile(r"(\d{10,})$")


def campaign_contact_id(cdp_id) -> int | None:
    if cdp_id is None:
        return None
    text = str(cdp_id).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    m = _TAIL.search(text)
    if m:
        return int(m.group(1))
    return int(hashlib.sha256(f"campaign-id:{text}".encode()).hexdigest()[:15], 16)


FUNCTIONS = {"campaign_contact_id": campaign_contact_id}
