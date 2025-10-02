# AGENTS.md

## Development workflow
- Work in relatively small, PR sized chunks

## Dev environment instructions
- Use `uv` for managing dependencies and environment

## General api philosophy
Each submodule of the library makes dealing with a GCP service in a ergonomic and opinionated way. 

- Auth is handled by Default Application Credentials
- Each submodule has its own `Client` class, which is a wrapper around the relevant GCP client
- The `Client` should have a `_gcp` attribute with the actual authorised client from google. This will allow to access all the official methods if needed.
- Each submodule has an `init()` function that returns an initialised client
- Each Client is callable i.e. has a `__call__` method defined with sane defaults. This should be used for the most common relevant function. For example the BigQuery client should use `__call__` to send a query, for the Secret Manager, it should be used to retrieve a secret
- The returned values of `__call__` methods shuold be immediately usable (a dataframe for a bigquery query, a text or json object for secretmanager etc.)
