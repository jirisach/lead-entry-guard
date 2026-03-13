"""Root conftest — makes the `tests` package importable as `tests.fixtures.common` etc."""
import sys
from pathlib import Path

# Add project root to sys.path so `from tests.fixtures.common import ...` works
sys.path.insert(0, str(Path(__file__).parent))
