# Coding Style Guidelines (Revised)

**Key philosophy**: Be concise, leverage Python's dynamic nature, avoid verbosity, and prioritize readability over strict adherence to conventions.

## Code Organization & Consolidation

- **Code should be compact and readable**: If something can be written inline and still be readable, do it - even if it's against PEP8 or common Python coding conventions. Aim at reducing noise while conveying enough information within context
- **Inline simple conditionals**: Use `if condition: statement` on a single line for simple cases (especially `if not x: raise`). Reserve multi-line blocks for complex logic
- **Ternary expressions for conditional assignments**: Prefer `x = a if condition else b` over multi-line if/else when assigning values
- **Remove unnecessary intermediate variables**: If `x = calculate(); return x` is just as readable as `return calculate()`, choose the latter
- **Extract conceptual duplication, not just repetition**: If two code paths do the same thing conceptually (even if the exact code appears only once in each), consolidate them. Don't wait for three exact copies. But don't be overzealous with DRY - unless we are talking about more than a simple couple of lines, it's probably worth abstracting
- **Reduce vertical space**: Consolidate related operations. `if x: a = b` and `if y: c = d` can be on separate lines without blank lines between them

## Be Synthetic

The code examples in the documentation will explain how things work. Developers using the library will have context about what they are doing and can always read the code. Keep implementation lean and let documentation provide the depth.

## Naming Conventions

- **Use well-known abbreviations**: `df` for DataFrame, `idx` for index, `ext` for extension, `o` for object - these are universally understood. Apply this to functions too: `df_to_schema_fields` not `dataframe_to_schema_fields`
- **Avoid over-abbreviation**: Don't use `d` for dict or `s` for string - these are too ambiguous - UNLESS the scope is very small (2-5 lines). The abbreviation should be immediately recognizable in context
- **Short names for clear iteration**: `for t in tables:` or `for o in objects:` is fine when the collection name makes the content obvious. Examples: `idx` for an index, `x, y` for coordinates
- **Collections should end with `s`**: Variables representing collections should normally end with an `s`. This applies to single letter variables as well (e.g. `ts = tables()`)

## Type Hints

- **Don't go overboard with type hints**: Python is a dynamic language. If a parameter accepts many different types, remove the type hints
- **Union 2-3 related types is fine**: `Path | str` or `int | float | None` are helpful. Beyond that, or with unrelated types, skip the hint
- **Don't type hint the obvious**: If a parameter is named `dataframe` or `df`, you probably don't need to annotate it
- **Use TYPE_CHECKING for type-only imports**: When an import is used exclusively for type checking, add `from typing import TYPE_CHECKING` and use top-level imports within an if statement

## Docstrings

- **Keep the docstrings short!** Coding examples belong to the library documentation, not the docstrings
- **Argument names should carry most of the weight**: Keep the argument description to a minimum
- **Don't enumerate every option**: If an argument expects one of a handful of possibilities, don't go overboard and describe each and every one of them
- **Properties should be fragments, not sentences**: `"""The fully qualified table ID."""` not `"""Get the fully qualified table ID.\n\nReturns:\n    The ID."""`
- **Skip docstrings when the signature is self-documenting**: If the function name and parameter names make it obvious, a docstring adds noise
- **Method docstrings: one line summary only**: Detailed parameter descriptions go in the docs site, not the code. Trust that developers will read the code if they need details

## Imports

- **Import at the narrowest useful scope**: Top of file for widely used, top of method for method-specific, but don't import the same thing multiple times in one method
- **Move repeated method-level imports to class-level**: If you're importing `bigquery` in 3+ methods, move it to the top

## Error Messages

- **Be specific and actionable**: Not just "Unsupported file format" but "Unsupported or undetected file format: Detected {ext}. Supported formats: ..."
- **Include context in errors**: Show what you found/received, not just what you expected

## Input Flexibility

- **Accept flexible types**: `Path | str` instead of just `Path`, let the implementation handle the conversion
- **Convert at point of use**: Don't convert inputs immediately, convert when you actually need the specific type

## General Philosophy

- **Python is a dynamic language. Let's take advantage of it**: We don't write Java in Python, we write Python
- **Readability through conciseness, not verbosity**: More lines ≠ more readable. Remove ceremony and get to the point
- **We don't need defensive coding patterns from static languages**: Trust that inputs will be reasonable and handle errors when they're not
- **Question every line**: Does this line add information or just ceremony? If ceremony, delete it
