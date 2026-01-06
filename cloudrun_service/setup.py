from setuptools import setup, find_packages

setup(
    name="csv_to_bq_pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "apache-beam[gcp]==2.57.0",
        "google-cloud-bigquery>=3.0.0",
        "google-cloud-storage>=2.0.0",
    ],
)
