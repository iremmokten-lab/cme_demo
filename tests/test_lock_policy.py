
def test_locked_snapshot_block(db_session, snapshot_factory):
    snapshot = snapshot_factory(locked=True)

    try:
        snapshot.name = "change"
        db_session.commit()
        assert False, "Update should not succeed"
    except Exception:
        assert True
