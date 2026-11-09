import setuptools

setuptools.setup(
    name="gcp-streaming-pipeline",
    version="1.0.0",
    install_requires=[
        "apache-beam[gcp]>=2.54.0",
        "pydantic>=2.5.0"
    ],
    packages=setuptools.find_packages(),
)
