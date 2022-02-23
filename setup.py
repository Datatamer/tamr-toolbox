"""Defines the package tamr_toolbox for user installations"""
from setuptools import setup, find_packages

with open("requirements.txt") as f:
    required = f.read().splitlines()

with open("version.txt") as f:
    full_version = f.read().splitlines()[0]
    version_number = full_version.split("-")[0]

with open("README.md", encoding="utf-8") as f:
    readme = f.read()

setup(
    name="tamr_toolbox",
    version=version_number,
    author="Tamr Inc.",
    author_email="",
    description="Tools for Tamr",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/Datatamer/tamr-toolbox",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.6",
    install_requires=required,
    extras_require={
        # Super set including all optional dependencies
        "all": [
            "pandas>=0.21.0",
            "aiohttp==3.7.4.post0",
            "slackclient==2.7.2",
            "responses==0.10.14",
            "google-cloud-translate==2.0.1",
            "paramiko>=2.8.0",
        ],
        # Individual sets of dependencies
        "pandas": ["pandas>=0.21.0"],
        "slack": ["aiohttp==3.7.4.post0", "slackclient==2.7.2"],
        "testing": ["responses==0.10.14"],
        "translation": ["google-cloud-translate==2.0.1"],
        "ssh": ["paramiko>=2.8.0"],
    },
)
