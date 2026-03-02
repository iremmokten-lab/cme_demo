def test_job_queue_import():
    from src.services import job_queue
    assert hasattr(job_queue, "enqueue")
