import sys
import subprocess
import importlib

# Try to install missing dependencies
try:
    import pydantic_settings
except ImportError:
    print("Installing pydantic-settings...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pydantic-settings==2.4.0"])
    import pydantic_settings
