def validate_engine_contracts():
    """
    Ensure all required public APIs exist.
    Fail early with clear errors.
    """

    import data.chart_week
    import data.index

    REQUIRED = {
        "data.chart_week": ["get_current_week_id"],
        "data.index": ["record_week_publish", "week_already_published"],
    }

    for module_name, attrs in REQUIRED.items():
        module = __import__(module_name, fromlist=["*"])
        for attr in attrs:
            if not hasattr(module, attr):
                raise RuntimeError(
                    f"Engine contract broken: {module_name}.{attr} missing"
                )