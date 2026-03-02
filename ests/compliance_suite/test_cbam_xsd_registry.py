def test_cbam_xsd_registry_import():
    from src.services import cbam_schema_registry as r
    assert hasattr(r, "fetch_and_cache_official_cbam_xsd_zip")
