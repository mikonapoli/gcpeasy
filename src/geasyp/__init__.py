"""geasyp: GCP, but easy"""

__version__ = "0.1.0"

# Import submodules to make them available at package level
from . import bq
from . import gcs
from . import secretmanager

__all__ = ["bq", "gcs", "secretmanager"]
