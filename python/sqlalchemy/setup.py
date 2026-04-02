"""Setup script for the PyroSQL SQLAlchemy dialect."""

from setuptools import setup, find_packages

setup(
    name="sqlalchemy-pyrosql",
    version="0.1.0",
    description="SQLAlchemy dialect for PyroSQL",
    long_description="A SQLAlchemy dialect that enables SQLAlchemy to work with PyroSQL databases.",
    author="PyroSQL Contributors",
    license="BSL-1.1",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "sqlalchemy>=2.0",
        "pyrosql",
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "pyrosql = pyrosql_dialect:dialect",
            "pyrosql.pyrosql = pyrosql_dialect:dialect",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
    ],
)
