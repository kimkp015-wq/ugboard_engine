# scripts/check_dependencies.py
import subprocess
import sys
import json
from typing import Dict, List, Tuple

def check_dependency_versions() -> Tuple[bool, List[str]]:
    """Check for dependency conflicts and outdated packages."""
    
    issues = []
    
    try:
        # Get installed packages
        result = subprocess.run(
            [sys.executable, "-m", "pip", "list", "--format=json"],
            capture_output=True,
            text=True,
            check=True
        )
        
        installed = json.loads(result.stdout)
        installed_dict = {pkg["name"].lower(): pkg["version"] for pkg in installed}
        
        # Critical dependencies to check
        critical_deps = {
            "fastapi": ("0.104.1", "Framework version"),
            "aiohttp-client-cache": ("0.14.3", "HTTP caching"),
            "python": ("3.11", "Python version"),
        }
        
        for dep, (expected_version, purpose) in critical_deps.items():
            if dep in installed_dict:
                if installed_dict[dep] != expected_version:
                    issues.append(f"⚠️  {dep} is {installed_dict[dep]}, expected {expected_version} ({purpose})")
            else:
                issues.append(f"❌ {dep} is not installed ({purpose})")
        
        return len(issues) == 0, issues
        
    except Exception as e:
        return False, [f"Error checking dependencies: {str(e)}"]

if __name__ == "__main__":
    success, issues = check_dependency_versions()
    
    print("=" * 60)
    print("UG Board Engine - Dependency Check")
    print("=" * 60)
    
    for issue in issues:
        print(issue)
    
    if success:
        print("✅ All dependencies are properly installed!")
        sys.exit(0)
    else:
        print(f"\n❌ Found {len(issues)} issues to resolve")
        sys.exit(1)
