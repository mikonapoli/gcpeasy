"""Input validation utilities for BigQuery."""

import re


def validate_identifier(identifier: str, name: str = "identifier") -> str:
    """Validate BigQuery identifier (table, dataset, project names).

    BigQuery identifiers must:
    - Table/Dataset: Start with letter/underscore, contain only letters, numbers, underscores
    - Project: Can contain letters, numbers, hyphens, apostrophes (6-30 chars for new projects)
    - Max 1024 characters

    Args:
        identifier: The identifier to validate.
        name: Name of the identifier for error messages.

    Returns:
        The validated identifier.

    Raises:
        ValueError: If identifier is invalid.
    """
    if not identifier:
        raise ValueError(f"{name} cannot be empty")

    if len(identifier) > 1024:
        raise ValueError(f"{name} cannot exceed 1024 characters")

    # Project IDs have different rules (can contain hyphens, apostrophes, underscores)
    if "project" in name.lower():
        # Project IDs: letters, numbers, hyphens, apostrophes, underscores, must start with letter
        if not re.match(r'^[a-z][a-z0-9\-\'_]*$', identifier, re.IGNORECASE):
            raise ValueError(
                f"Invalid {name}: '{identifier}'. "
                "Must start with a letter and contain only letters, numbers, hyphens, apostrophes, and underscores."
            )
    else:
        # Table/Dataset IDs: start with letter/underscore, contain only alphanumeric/underscore
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValueError(
                f"Invalid {name}: '{identifier}'. "
                "Must start with letter or underscore and contain only letters, numbers, and underscores."
            )

    return identifier
