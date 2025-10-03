# AGENTS.md

## Development workflow
- Work in relatively small, PR sized chunks
- Test the code with `make test` each time you make a code change
- Whenever you make a code change that adds or modifies the behavior of the code, you should add a test for that behavior
- Use `uv` for managing dependencies and environment

## Coding style
The key philosophy is: be concise, leverage Python's dynamic nature, avoid verbosity, and prioritize readability of strict adherence to conventions.

- Code should be compact and readable. If something can be written inline and still be readable do it, even if it's against PEP8 or common Python coding conventions. Aim at reducing noise, while conveying enough information within context.
- Be synthetic! The code exmamples in the documentation will explain how things work. Developers using the library will have context about what they are doing and can always read the code
- Don't go overboard with type hints. If a parameter accepts many different types, remove the type hints. Python is a dynamic language
- Keep the docstrings short! Coding examples belong to the library documentation, not the docstrings.
    - Argument names should carry most of the weight, keep the argument description to a minimum
    - If an argument expects one of a handful of possibilities don't go overboard and describe each and everyone of them
- When an import is used exclusively for type checking add `from typing import TYPE_CHECKING` and use top-level imports within an if statement
- Python is a dynamic language. Let's take advantage of it. We don't write Java in Python, we write Python.
- Short variables, function and method names are ok when it's clear what they mean. This is especially true for common iteration (e.g. `idx` for an index, `x, y` for coordinates, `o` for an object). If the iterator makes it clear what it contains, one letter variables are ok: `for t in tables:` it's perfectly acceptable
- Variables representing collections shuold normally end with an `s`. This applies to single letter variables as well (e.g. `ts = tables()`)

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

## General API philosophy
Each submodule of the library makes dealing with a GCP service in a ergonomic and opinionated way. 

- Auth is handled by Default Application Credentials
- Each submodule has its own `Client` class, which is a wrapper around the relevant GCP client
- The `Client` should have a `_gcp` attribute with the actual authorised client from google. This will allow to access all the official methods if needed.
- Each submodule has an `init()` function that returns an initialised client
- Each Client is callable i.e. has a `__call__` method defined with sane defaults. This should be used for the most common relevant function. For example the BigQuery client should use `__call__` to send a query, for the Secret Manager, it should be used to retrieve a secret
- The returned values of `__call__` methods shuold be immediately usable (a dataframe for a bigquery query, a text or json object for secretmanager etc.)
