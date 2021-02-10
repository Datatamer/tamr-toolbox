"""Defines the package Tamr-Toolbox for user installations"""
from setuptools import setup, find_packages

with open("requirements.txt") as f:
    required = f.read().splitlines()

with open("version.txt") as f:
    full_version = f.read().splitlines()[0]
    version_number = full_version.split("-")[0]

setup(
    name="tamr_toolbox",
    version=version_number,
    author="Tamr Inc.",
    author_email="",
    description="Tools for Tamr",
    long_description_content_type="text/markdown",
    url="https://github.com/Datatamer/tamr-toolbox",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.6",
    install_requires=required,
    extras_require={
        # Super set including all optional dependencies
        "all": [
            "pandas>=0.21.0",
            "slackclient>=2.7.2",
            "responses==0.10.14",
            "google-cloud-translate==2.0.1",
        ],
        # Individual sets of dependencies
        "pandas": ["pandas>=0.21.0"],
        "slack": ["slackclient>=2.7.2"],
        "testing": ["responses==0.10.14"],
        "translation": ["google-cloud-translate==2.0.1"],
    },
)
