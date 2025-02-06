from setuptools import setup, find_packages

setup(
    name='at_sn_dataflow_pipeline',
    version='0.1.0',
    description='Dataflow pipeline for AT Scrub Number data',
    author='Kelvin Rojas',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]>=2.45.0',
        'google-cloud-storage>=2.6.0',
        'google-cloud-firestore>=2.7.0',
        'requests>=2.28.0'
    ],
    entry_points={
        'console_scripts': [
            'at_sn_dataflow_pipeline = at_sn_dataflow_pipeline.pipeline:run'
        ]
    }
)