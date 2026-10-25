from contextlib import contextmanager


def available() -> bool:
    try:
        import mlflow  # noqa: F401
    except ImportError:
        return False
    return True


@contextmanager
def trace_span(name: str, **attributes: object):
    try:
        import mlflow
    except ImportError:
        yield None
        return
    with mlflow.start_span(name=name) as span:
        for key, value in attributes.items():
            span.set_attribute(key, str(value))
        yield span


def log_event(span, attributes: dict[str, object]) -> None:
    if span is None:
        return
    for key, value in attributes.items():
        span.set_attribute(key, str(value))
