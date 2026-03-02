def test_ets_monitoring_plan_import():
    from src.services.ets_monitoring_plan import upsert_monitoring_plan
    assert callable(upsert_monitoring_plan)
