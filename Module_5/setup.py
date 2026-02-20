"""Setuptools configuration for Module_5."""

from pathlib import Path

from setuptools import find_packages, setup


ROOT = Path(__file__).resolve().parent


def read_requirements(relative_path: str):
    """Read dependency lines from a requirements file."""

    requirements_path = ROOT / relative_path
    if not requirements_path.exists():
        return []

    requirements = []
    for line in requirements_path.read_text(encoding="utf-8").splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        requirements.append(item)
    return requirements


setup(
    name="module5-grad-cafe",
    version="0.1.0",
    description="Grad Cafe analytics application (Module_5)",
    long_description=(ROOT / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    packages=find_packages(include=["src", "src.*"]),
    include_package_data=True,
    package_data={
        "src": [
            "templates/*.html",
            "static/*.css",
            "*.json",
            "llm_hosting/*.txt",
            "llm_hosting/*.json",
        ]
    },
    install_requires=read_requirements("requirements.txt"),
    python_requires=">=3.13",
)
