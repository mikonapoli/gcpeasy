"""Input validation utilities for BigQuery."""

import re


def validate_identifier(identifier: str, name: str = "identifier") -> str:
    """Validate BigQuery identifier (table, dataset, project names)."""
    if not identifier: raise ValueError(f"{name} cannot be empty")
    if len(identifier) > 1024: raise ValueError(f"{name} cannot exceed 1024 characters")

    if "project" in name.lower():
        if not re.match(r'^[a-z][a-z0-9\-\'_]*$', identifier, re.IGNORECASE):
            raise ValueError(f"Invalid {name}: '{identifier}'. Must start with a letter and contain only letters, numbers, hyphens, apostrophes, and underscores.")
    else:
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValueError(f"Invalid {name}: '{identifier}'. Must start with letter or underscore and contain only letters, numbers, and underscores.")

    return identifier
