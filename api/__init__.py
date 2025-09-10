from pathlib import Path
import sys

# Garante que a pasta 'app' esteja no PYTHONPATH
_current_dir = Path(__file__).resolve().parent
_app_dir = _current_dir.parent / "app"
_app_dir_str = str(_app_dir)

if _app_dir.exists() and _app_dir_str not in sys.path:
    sys.path.insert(0, _app_dir_str)
