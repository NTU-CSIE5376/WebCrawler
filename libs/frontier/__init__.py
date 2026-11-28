"""Shared URL policy with explicitly selected Python or PyArrow evaluation."""
from .trap_rules import URLPolicy

__all__ = ['URLPolicy', 'evaluate']


def evaluate(urls, policy=None, *, backend='python'):
    """Evaluate URLs in input order using the selected implementation.

    python (default): iterable of str/None -> list of decision dictionaries.
    pyarrow: Arrow string Array/ChunkedArray -> Arrow decision table.

    Both implementations expose identical logical decision fields. Selecting
    Python never imports PyArrow. Existing scalar helpers retain their APIs.
    """
    if backend == 'python':
        from .python_backend import evaluate as implementation
    elif backend == 'pyarrow':
        from .columnar_backend import evaluate as implementation
    else:
        raise ValueError("backend must be 'python' or 'pyarrow'")
    return implementation(urls, URLPolicy() if policy is None else policy)
