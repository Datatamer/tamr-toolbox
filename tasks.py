"""Defines useful development functions, available using the invoke command"""
from pathlib import Path

from invoke import task


def _find_packages(path: Path):
    for pkg in path.iterdir():
        if pkg.is_dir() and len(list(pkg.glob("**/*.py"))) >= 1:
            yield pkg


def _find_scripts(path: Path):
    return path.glob("**/*.py")


@task
def lint(c):
    """Uses flake8 to report linting errors in your code"""
    c.run("flake8 .", echo=True, pty=True)


@task
def format(c, fix=False, diff=False):
    """Uses black to report any formatting issues in your code

    Args:
        fix: Flag to automatically fix formatting issues in your code
        diff: Flag to include a diff between your current code and the recommended code
    """
    if fix and diff:
        print("Please use only --diff OR --fix, but not both, when calling format.")
    else:
        if fix:
            arg = ""
        elif diff:
            arg = "--diff"
        else:
            arg = "--check"

        c.run(f"black {arg} --line-length=99 .", echo=True, pty=True)


@task
def test(c, path=None):
    """Uses pytest to run the tests you have written for your code.

    Args:
        path: Flag to specify the path to a subset of tests to run
    """
    arg = path if path is not None else ""
    c.run(f"python -m pytest {arg}", echo=True, pty=True)


@task
def docs(c):
    """Uses Sphinx to built Tamr-toolbox documentation html"""
    c.run(
        f"sphinx-build -b html doc_src docs/_draft_build -W",
        echo=True,
        pty=True,
        env={"TAMR_TOOLBOX_DOCS": "1"},
    )
