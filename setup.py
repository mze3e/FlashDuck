"""
Setup script for DuckRedis package
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="duckredis",
    version="0.1.0",
    author="DuckRedis Contributors",
    author_email="contact@duckredis.org",
    description="High-performance data management combining DuckDB and Redis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/duckredis/duckredis",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=[
        "redis>=4.0.0",
        "duckdb>=0.8.0",
        "pandas>=1.3.0",
        "pyarrow>=8.0.0",
        "click>=8.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=3.0",
            "black>=22.0",
            "flake8>=4.0",
            "mypy>=0.950",
        ],
        "streamlit": [
            "streamlit>=1.28.0",
            "plotly>=5.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "duckredis=duckredis.cli:cli",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
