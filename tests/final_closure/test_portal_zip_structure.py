from src.services.portal_readiness import validate_portal_zip_structure

def test_zip_structure_rejects_non_zip():
    ok, errors, warnings, meta = validate_portal_zip_structure(b"not a zip")
    assert ok is False
    assert len(errors) >= 1
