from setuptools import setup, find_packages

setup(
    name="clearcare-compliance",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "jsonschema>=4.19",
        "pandas>=2.2",
        "pyyaml>=6.0",
        "rich>=13.7",
        "python-dateutil>=2.9",
        "click>=8.1"
    ],
    entry_points={
        "console_scripts": [
            "clearcare-validate=clearcare_compliance.cli:main",
        ],
    },
)
