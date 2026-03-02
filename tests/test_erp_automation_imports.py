def test_erp_automation_imports():
    import src.db.erp_automation_models  # noqa: F401
    import src.erp_automation.orchestrator  # noqa: F401
    import src.erp_automation.connectors.generic_rest  # noqa: F401
