"""Python setup.py for project package."""

import os
from setuptools import find_packages, setup


def read(*paths, **kwargs):
    """Read the contents of a text file safely."""
    content = ""
    with open(
        os.path.join(os.path.dirname(__file__), *paths),
        encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


def read_requirements(path):
    """Read requirements from a .txt file."""
    return [
        line.strip()
        for line in read(path).split("\n")
        if not line.startswith(('"', "#", "-", "git+"))
    ]


setup(
    name="project_name",
    version=read("VERSION"),
    description="project_description",
    url="github_url",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    license=read("LICENSE"),
    author="NOMA AI INC.",
    packages=find_packages(exclude=["tests", ".github"]),
    install_requires=read_requirements("requirements.txt"),
    extras_require={"dev": read_requirements("requirements-dev.txt")},
)
