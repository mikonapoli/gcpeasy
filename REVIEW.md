# Code & Test Review

This review summarizes the issues discovered while checking the repository against the documented coding and testing guidelines, along with the rationale and concrete suggestions for remediation. References point to the exact files and lines observed at review time.

## Critical Behavioural Bugs

1. **`Table.read` passes unsupported keyword argument**  
   - **Observation:** The method calls `self._client.query(query, params=params)` when `max_results` is provided (`src/gcpeasy/bq/table.py:73`). The real BigQuery client accepts `job_config` and `job_config.query_parameters`, not a `params` keyword.  
   - **Impact:** Any real query with `max_results` will raise `TypeError: query() got an unexpected keyword argument 'params'`, breaking consumers in production.  
   - **Guideline conflict:** Behavioural regression risk; tests should catch this (testing guidelines).  
   - **Suggested change:** Build a `QueryJobConfig` on the fly when parameters are supplied. Example structure:  
     ```python
     job_config = QueryJobConfig()
     job_config.query_parameters = [ScalarQueryParameter('max_results', 'INT64', int(max_results))]
     result = self._client.query(query, job_config=job_config)
     ```
     Keep the ergonomics lean by only instantiating the config when needed.

2. **Tests encode the same invalid API usage**  
   - **Observation:** `tests/test_bq_table.py:62` asserts that the mocked client received the unsupported `params` argument, so the test suite currently enshrines the bug. Similar style appears across the tests where internal Google client methods are patched and call signatures inspected.  
   - **Impact:** The suite cannot catch regressions in actual behaviour and actively prevents fixing the bug without rewriting the test.  
   - **Guideline conflict:** Violates “test behaviour, not implementation” and “tests should make failures obvious.”  
   - **Suggested change:** Rework the tests to validate the dataframe the method returns (observable output) while providing a fake client that mimics the `google.cloud.bigquery.Client` surface but accepts correct arguments. Upon fixing the production code, adjust the tests to assert the job config’s parameters via the fake rather than checking for a `params` keyword.

## Testing Guideline Gaps

1. **Over-reliance on mocks and internal call assertions**  
   - **Locations:** Widespread in `tests/test_bq_client.py`, `tests/test_bq_table.py`, `tests/test_bq_dataset.py`, `tests/test_bq_file_loading.py`, etc.  
   - **Issue:** Most tests patch `gcpeasy.bq.client.bigquery.Client` and verify internal calls (`assert_called_once_with`, inspecting keyword args). This verifies wiring rather than behaviour, making tests brittle and less informative.  
   - **Recommendation:** Introduce lightweight fakes or stub objects that emulate the BigQuery client’s public surface and capture state (e.g., the executed SQL, job configs, inserted rows). Then assert against observable outputs—returned dataframes, schema conversions, raised errors—per guideline “tests should test behaviour, not implementation.”  
   - **Rationale:** Behavioural tests will still pass if Google’s API introduces compatible changes, and they will fail with clear messages when the user-facing contract breaks.

2. **Missing regression coverage for critical flows**  
   - **Observation:** There is no test exercising `Client.load_data` end-to-end or verifying that `Table.write` creates proper load jobs in all branches (especially the `None`/empty-table path).  
   - **Recommendation:** Add regression tests that cover each major branch of `Table.write` (dataframe, GCS URI, local file, `None + schema`) and `Client.load_data` to ensure the correct `Table` helper is invoked. Each test should focus on a single observable outcome, matching the “one thing per test” rule.

## Coding Style Deviations

1. **Verbose docstrings contrary to style guide**  
   - **Observation:** Many docstrings include multi-line summaries and full `Args`/`Returns` sections (`src/gcpeasy/bq/table.py:24`, `src/gcpeasy/bq/dataset.py:11`, `tests/test_bq_client.py:1`).  
   - **Guideline conflict:** Documentation directs us to keep docstrings short—often a single line—and avoid enumerating arguments unless necessary.  
   - **Suggested change:** Compress docstrings to succinct one-liners where the signature is already descriptive. Reserve detailed commentary for external docs.

2. **Extraneous vertical whitespace and verbose blocks**  
   - **Observation:** Some methods (e.g., `Table.write`, `Dataset.create`) still use multi-line `if` blocks with blank lines where concise inline statements would be clearer, given the project’s “compact and readable” philosophy.  
   - **Suggested change:** Collapse simple conditionals (`if location: ds.location = location`) consistently, ensure related operations are adjacent without superfluous blank lines, and prefer expression-based returns where readability allows.

## Implementation Notes for Proposed Fixes

- **Fixing `Table.read`:**  
  - Create or reuse a helper that produces `QueryJobConfig` objects with scalar parameters.  
  - Coerce `max_results` to an integer before building the job config; raise a clear `ValueError` if coercion fails (existing behaviour already does this).  
  - Update tests to expect a `QueryJobConfig` and inspect its `query_parameters` via the fake client.

- **Rewriting tests around behaviour:**  
  - Replace `patch("gcpeasy.bq.client.bigquery.Client")` blocks with dependency injection: instantiate `Client`/`Table` using simple stand-ins that track inputs and return configured results.  
  - Keep each test focused on a single expectation and ensure failure messages highlight the user-facing contract breach.  
  - Where mocking is unavoidable (e.g., file I/O), keep the patch local and still assert on results rather than call signatures.

- **Docstring & formatting clean-up:**  
  - Convert verbose docstrings to one-liners, remove redundant argument descriptions, and eliminate empty vertical space.  
  - Apply these adjustments consistently across modules to avoid stylistic drift.

- **Completing `gcs`/`secretmanager`:**  
  - Follow the BigQuery blueprint: add `Client` (wrapping the relevant Google client), an `init()` helper, a callable `__call__` for the primary action, and expose them via `__all__`.  
  - Add smoke tests to confirm `init()` returns the wrapper and `__call__` delegates correctly.

