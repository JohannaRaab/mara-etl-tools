from setuptools import setup, find_packages

setup(
    name='etl-tools',
    version='1.0.1',

    description='Utilities for creating ETL pipelines with mara',

    install_requires=[
        'mara-config>=0.1'
        'data_integration>=1.0.0',
    ],

    dependency_links=[
        'git+https://github.com/mara/mara-config.git@0.1#egg=mara-config-0.1',
        'git+https://github.com/mara/data-integration.git@1.0.0#egg=data-integration-1.0.0'
    ],

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    entry_points={},
)

