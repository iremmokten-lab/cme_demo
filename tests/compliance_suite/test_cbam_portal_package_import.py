def test_cbam_portal_package_import():
    from src.services.cbam_portal_package import build_cbam_portal_package
    assert callable(build_cbam_portal_package)
