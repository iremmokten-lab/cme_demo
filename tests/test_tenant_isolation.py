
def test_tenant_isolation(client_a, client_b, snapshot_factory):
    snap = snapshot_factory(tenant_id="A")

    res = client_b.get(f"/snapshots/{snap.id}")
    assert res.status_code in (401,403,404)
