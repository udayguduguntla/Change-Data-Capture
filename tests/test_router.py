
from src.router import ControlEvent, Quarantined, Router
from tests.fakes import FakeRegistry, envelope, value_schema

CONTACT_SCHEMA = value_schema({
    "contact_id": "int64", "tenant_id": "string", "email": "string",
    "created_at": "io.debezium.time.MicroTimestamp",
    "lifetime_value": "io.debezium.data.VariableScaleDecimal",
})


def contact(tenant="tenant-alpha", cid=1, **extra):
    row = {"contact_id": cid, "tenant_id": tenant, "first_name": "Ada",
           "last_name": "Lovelace", "email": "ada@example.com", "status": "active",
           "account_id": 99}
    row.update(extra)
    return row


# --- topic parsing ---------------------------------------------------------

def test_topic_maps_to_table(router):
    assert router.table_for("cdc.crm.contacts") == "crm.contacts"


def test_control_topic_detected(router):
    assert router.is_control_topic("cdc.accounts.organisations")
    assert not router.is_control_topic("cdc.crm.contacts")


def test_control_topic_returns_a_control_event(router):
    d = router.route("cdc.accounts.organisations",
                     envelope(after={"organization_id": "x", "tenant_id": "t",
                                     "status": "active"}))
    assert isinstance(d, ControlEvent) and d.op == "c"


# --- the happy path --------------------------------------------------------


# --- ACCEPTANCE TEST 6: cross-tenant isolation -----------------------------


# --- fail-closed (spec 8.3) ------------------------------------------------

def test_unknown_tenant_is_quarantined_never_guessed(router):
    d = router.route("cdc.crm.contacts",
                     envelope(after=contact("tenant-nobody"), schema=CONTACT_SCHEMA))
    assert isinstance(d, Quarantined) and d.reason == "unknown_tenant"


def test_missing_tenant_id_is_quarantined(router):
    row = contact()
    del row["tenant_id"]
    d = router.route("cdc.crm.contacts", envelope(after=row, schema=CONTACT_SCHEMA))
    assert isinstance(d, Quarantined) and d.reason == "missing_tenant_id"


def test_inactive_organisation_is_quarantined(loader, org_a):
    org_a._active = False
    r = Router(loader, FakeRegistry([org_a]), "cdc", "accounts.organisations")
    d = r.route("cdc.crm.contacts", envelope(after=contact(), schema=CONTACT_SCHEMA))
    assert isinstance(d, Quarantined) and d.reason == "organisation_inactive"


def test_unprovisioned_organisation_is_quarantined_and_replayable(loader, org_a):
    org_a.provisioned = False
    r = Router(loader, FakeRegistry([org_a]), "cdc", "accounts.organisations")
    d = r.route("cdc.crm.contacts", envelope(after=contact(), schema=CONTACT_SCHEMA))
    assert isinstance(d, Quarantined) and d.reason == "organisation_not_provisioned"


def test_unknown_table_is_quarantined(router):
    d = router.route("cdc.webtrack.pageviews",
                     envelope(after={"tenant_id": "tenant-alpha", "id": 1}))
    assert isinstance(d, Quarantined) and d.reason == "unknown_table"


def test_delete_without_a_before_image_names_replica_identity(router):
    """
    Under REPLICA IDENTITY DEFAULT the before-image is PK-only, so tenant_id is absent and every delete would be quarantined. The message has to say so.
    """
    d = router.route("cdc.crm.contacts", envelope(before=None, op="d"))
    assert isinstance(d, Quarantined)
    assert "REPLICA IDENTITY" in d.detail


def test_pk_only_before_image_is_quarantined_with_a_useful_message(router):
    d = router.route("cdc.crm.contacts", envelope(before={"contact_id": 5}, op="d"))
    assert isinstance(d, Quarantined) and d.reason == "missing_tenant_id"
    assert "REPLICA IDENTITY" in d.detail


def test_unknown_operation_is_quarantined(router):
    d = router.route("cdc.crm.contacts", envelope(after=contact(), op="?"))
    assert isinstance(d, Quarantined) and d.reason == "unknown_operation"


# --- ACCEPTANCE TEST 8: product entitlement --------------------------------


# --- payload assembly ------------------------------------------------------


# --- buffer key ------------------------------------------------------------


# --- schema drift ----------------------------------------------------------


# --- configurable tenant discriminator --------------------------------------


def test_wrong_discriminator_column_quarantines_with_the_column_name(loader, org_a):
    r = Router(loader, FakeRegistry([org_a]), "cdc", "accounts.organizations",
               tenant_column="organization_id")
    d = r.route("cdc.crm.contacts",
                envelope(after={"contact_id": 1, "tenant_id": "tenant-alpha"}))
    assert isinstance(d, Quarantined) and d.reason == "missing_tenant_id"
    assert "organization_id" in d.detail      # names the column actually looked for
