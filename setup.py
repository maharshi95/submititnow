import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='submititnow',
    version='0.0.1',
    author='Mahrshi Gor',
    author_email='maharshigor@gmail.com',
    description='A package to make submitit easier to use',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/maharshi95/submititnow',
    project_urls = {
        "Bug Tracker": "https://github.com/maharshi95/submititnow/issues",
    },
    license='MIT',
    packages=['submititnow'],
    scripts=['bin/jt'],
    install_requires=['submitit', 'simple_colors', 'pandas'],
)