#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="sumo-tableaggregator",
    description="Python package for interacting with Sumo in an FMU setting",
    url="https://github.com/equinor/sumo-tableaggregator",
    use_scm_version={"write_to": "src/sumo/table_aggregation/version.py"},
    author="Equinor",
    license="GPLv3",
    keywords="fmu, sumo",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    author_email="dbs@equinor.com",
    # entry_points={
    #     "ert": [
    #         "fmu_sumo_jobs = fmu.sumo.hook_implementations.jobs",
    #         "sumo_upload = fmu.sumo.uploader.scripts.sumo_upload",
    #     ],
    #     "console_scripts": ["sumo_upload=fmu.sumo.uploader.scripts.sumo_upload:main"],
    # },
    install_requires=[
        "PyYAML",
        "pandas",
        "sumo-wrapper-python",
        "setuptools",
        "azure-core",
        "deprecation",
    ],
    python_requires=">=3.8",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    zip_safe=False,
)
