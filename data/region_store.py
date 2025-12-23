def publish_region(region: str):
    """
    Publish = lock region.
    Snapshot logic handled elsewhere.
    """
    lock_region(region)