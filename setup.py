from setuptools import find_packages, setup

__package_name__ = "legiti_challenge"
__version__ = "0.1.0"
__repository_url__ = "https://github.com/rafaelleinio/legiti-challenge"

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

with open("README.md") as f:
    long_description = f.read()

setup(
    name=__package_name__,
    description="Repository with the solution for the 2021's Legiti code challenge.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="legiti challenge",
    version=__version__,
    url=__repository_url__,
    packages=find_packages(
        exclude=(
            "docs",
            "tests",
            "tests.*",
            "pipenv",
            "env",
            "examples",
            "htmlcov",
            ".pytest_cache",
        )
    ),
    license="MIT",
    author="Rafael Leinio Pereira",
    install_requires=requirements,
    python_requires=">=3.7, <4",
)
