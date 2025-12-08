import subprocess
from pathlib import Path


def test_sqlite_extension_compilation():
    parent = Path(__file__).parent.absolute()
    cmd = f"cd {parent}; gcc -g -fPIC -dynamiclib rot13.c -o rot13.dylib"
    result = subprocess.run(cmd, shell=True)
    assert result.returncode == 0, f'"{cmd}" failed'
