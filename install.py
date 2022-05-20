"""Install requirements for development of the Tamr Toolbox"""
from pathlib import Path
from subprocess import run


def main(*, python_exec: Path) -> None:
    """Pip installs project dependencies

    Args:
        python_exec: path to python executable

    """
    run([str(python_exec), "-m", "pip", "install", "--upgrade", "pip==21.3.1"])
    run([str(python_exec), "-m", "pip", "install", "--upgrade", "setuptools==45.1.0"])
    run([str(python_exec), "-m", "pip", "install", "-r", "dev_requirements.txt"])
    run([str(python_exec), "-m", "pip", "install", "-r", "optional_requirements.txt"])
    run([str(python_exec), "-m", "pip", "install", "--editable", "."])
    print("Tamr toolbox and development dependencies installed.")


def enforce_python_version() -> Path:
    """Returns the python executable if it meets version requirements

    Returns: path to python executable

    """
    import sys

    py_ver = sys.version_info
    fpy_ver = "{}.{}.{}".format(py_ver.major, py_ver.minor, py_ver.micro)
    if py_ver.major != 3:
        print(
            "Error: Requires Python 3.6+. Your version:{}. "
            "Try using 'python3' instead of 'python' in your command.".format(fpy_ver)
        )
        sys.exit(1)
    elif py_ver.minor < 6:
        print("Error: Requires Python 3.6+. Your version:", fpy_ver)
        sys.exit(1)
    return Path(sys.executable)


if __name__ == "__main__":
    main(python_exec=enforce_python_version())
