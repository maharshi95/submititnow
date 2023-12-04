import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

packages = filter(lambda x: x.startswith("submititnow"), setuptools.find_packages())

setuptools.setup(
    name="submititnow",
    version="0.9.4.1",
    author="Maharshi Gor",
    author_email="maharshigor@gmail.com",
    description="A package to make submitit easier to use",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/maharshi95/submititnow",
    project_urls={
        "Bug Tracker": "https://github.com/maharshi95/submititnow/issues",
    },
    license="MIT",
    packages=list(packages),
    scripts=["bin/jt", "bin/slaunch", "bin/py-srun"],
    install_requires=[
        "submitit==1.4.5",
        "pandas>=1.5.0",
        "typer[all]>=0.7.0",
        "rich-cli>=1.8.0",
        "rich>=12.6.0",
        "tqdm>=4.0.0",
        "scandir>=1.10.0",
    ],
    python_requires=">=3.8",
)
