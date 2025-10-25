"""Setup script for DataWorkflow SDK."""
from setuptools import setup, find_packages

setup(
    name="dataworkflow",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "flask",
        "requests",
        "sqlalchemy",
        "alembic",
        "pydantic",
    ],
    python_requires=">=3.10",
)
