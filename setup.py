"""
Setup configuration for MF_ETL package
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="mf-etl",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Mutual Fund ETL and Enrichment Service",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/shiv159/MF_ETL",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
            "black>=22.0",
            "flake8>=4.0",
            "mypy>=0.950",
            "isort>=5.10",
        ],
        "api": [
            "fastapi>=0.95",
            "uvicorn>=0.21",
        ],
    },
    entry_points={
        "console_scripts": [
            "mf-etl-api=services.api.main:run_api",
            "mf-etl-demo=demos.end_to_end_demo:main",
        ],
    },
)
