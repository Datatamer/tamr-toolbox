"""Defines the package tamr_toolbox for user installations"""

from setuptools import setup, find_packages

with open("requirements.txt") as f:
    required = f.read().splitlines()

with open("version.txt") as f:
    full_version = f.read().splitlines()[0]
    version_number = full_version.split("-")[0]

with open("README.md", encoding="utf-8") as f:
    readme = f.read()

# Individual sets of optional dependencies. This is the single source of truth for optional
# dependencies; the "all" extra is derived from it below.
optional_dependencies = {
    "address-validation": ["googlemaps>=4.10.0"],
    "pandas": ["pandas>=2.2.2,<3"],
    "slack": ["slack_sdk>=3.19"],
    "testing": ["responses>=0.24"],
    "translation": ["google-cloud-translate>=3.15"],
    "ssh": ["paramiko>=3.4"],
    "gcs": ["google-cloud-storage>=2.0.0"],
    "s3": ["boto3>=1.21.21", "boto3-stubs-lite[essential]>=1.21.21"],
}

setup(
    name="tamr_toolbox",
    version=version_number,
    author="Tamr Inc.",
    author_email="",
    description="Tools for Tamr",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/Datatamer/tamr-toolbox",
    license="Apache-2.0",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    ],
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.10",
    install_requires=required,
    extras_require={
        # Super set including all optional dependencies
        "all": sorted({dep for deps in optional_dependencies.values() for dep in deps}),
        **optional_dependencies,
    },
)
