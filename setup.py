from setuptools import setup, find_packages

setup(
    name='hello_airflow',
    version='latest',
    packages=find_packages(exclude=['tests']),
    package_dir={'': '.'},
    description='Hello Airflow',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ]
)