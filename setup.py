# coding=utf-8
from pathlib import Path

from setuptools import find_packages, setup

project_root = Path(__file__).parent

setup(
    name='plz',
    version='0.1',
    description='Helpers for running native Python functions on the CLSP grid.',
    author='Piotr Żelasko',
    author_email="pzelasko@jhu.edu",
    long_description=(project_root / 'README.md').read_text(),
    long_description_content_type="text/markdown",
    license='Apache-2.0 License',
    packages=find_packages(),
    install_requires=[
        'dask',
        'dask_jobqueue'
    ],
)
