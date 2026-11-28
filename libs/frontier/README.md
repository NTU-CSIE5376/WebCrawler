# URL policy

Two implementations share `URLPolicy`, thresholds, parameter sets and regex
definitions. Neither implementation performs logging, caching or database I/O.

```python
from libs.frontier import URLPolicy, evaluate

policy = URLPolicy()
urls = ["https://example.com/a?srsltid=x", "https://example.com/x/x/x/"]

python_decisions = evaluate(urls, policy)  # backend="python" by default

import pyarrow as pa
arrow_decisions = evaluate(pa.array(urls, type=pa.string()), policy, backend="pyarrow")
assert arrow_decisions.to_pylist() == python_decisions
```

Each decision contains the original URL, normalized URL, optional shadow URL,
a boolean column for each enabled rule, and `dropped`. Both implementations
preserve input order. `None` and empty URLs pass through without rule hits.
The keyword-only `backend` accepts `"python"` or `"pyarrow"`; other values
raise `ValueError`.

The Python implementation accepts any iterable and uses only the standard
library. The columnar implementation accepts Arrow string arrays and returns
an Arrow table; it parses distinct URLs in bounded batches and evaluates rule
masks using Arrow kernels. Importing this package or using the Python backend
does not require PyArrow.

Existing callers of `trap_rules.match_rules(url)`,
`url_normalize.strip_named_params(url, names)` and `url_normalize.strip_params(url, ...)`
keep their existing signatures and return values; no backend argument is needed.
Configuration, defaults, rule definitions and the Python matcher live together
in `trap_rules.py`.
Service adapters own audit logging and decision-cache lifecycles.
