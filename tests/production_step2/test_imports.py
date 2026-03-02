def test_imports_step2():
    import src.db.production_step2_models  # noqa: F401
    import src.services.job_queue  # noqa: F401
    import src.services.worker  # noqa: F401
    import src.services.cache_layer  # noqa: F401
    import src.services.support_bundle  # noqa: F401
