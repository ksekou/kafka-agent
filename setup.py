from setuptools import find_packages
from setuptools import setup


try:
    README = open("README.md").read()
except IOError:
    README = None

try:
    VERSION = open("VERSION").read().strip()
except IOError:
    VERSION = None

setup(
    name="kafka-agent",
    version=VERSION,
    description="Kafka client library",
    long_description=README,
    install_requires=["aiokafka==0.5.2"],
    author="Sekou Oumar",
    author_email="sekou@onna.com",
    url="",
    packages=find_packages(exclude=["demo"]),
    include_package_data=True,
    tests_require=["pytest"],
    extras_require={"test": ["pytest"]},
    classifiers=[],
    entry_points={},
)
