# AGENTS.md

## Development workflow
- Work in relatively small, PR sized chunks
- Test the code with `make test` each time you make a code change
- Whenever you make a code change that adds or modifies the behavior of the code, you should add a test for that behavior
- Use `uv` for managing dependencies and environment
- When we find and fix a bug for existing code, we add regression tests if relevant

## Testing guidelines
- Each test should test exactly one thing (ideally with a single assertion)
- It should be obvious what a test is checking
- When a test fails, it should be obvious what went wrong
- Test names should make easy to undersand what the test is checking and what expected behavior failed if the test does not pass
- Based on the previous point prefer test names similar `test_doing_this_should_result_in_that`
- Tests should always be written from the "outside in" and test expected behavior from the user of the system under test, rather than its implementation
- In other words: test the behavior, not the implementation, tehst the "what", not the "how"
- Tests should be limited to public interfaces
- Prefer dependency injections to mocks when possible
- Regression tests should still be written to preserve behavior, not the implementation

## General API philosophy
Each submodule of the library makes dealing with a GCP service in a ergonomic and opinionated way. 

- Auth is handled by Default Application Credentials
- Each submodule has its own `Client` class, which is a wrapper around the relevant GCP client
- The `Client` should have a `_gcp` attribute with the actual authorised client from google. This will allow to access all the official methods if needed.
- Each submodule has an `init()` function that returns an initialised client
- Each Client is callable i.e. has a `__call__` method defined with sane defaults. This should be used for the most common relevant function. For example the BigQuery client should use `__call__` to send a query, for the Secret Manager, it should be used to retrieve a secret
- The returned values of `__call__` methods shuold be immediately usable (a dataframe for a bigquery query, a text or json object for secretmanager etc.)
