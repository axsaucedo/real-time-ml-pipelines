from setuptools import setup, Command, find_packages
import os
import sys

if sys.version_info < (3,0):
    sys.exit('\nSorry, Python < 3.0 is not supported\nIf you have Python 3.x installed use: pip3 install xai')
    sys.exit('')

currentFileDirectory = os.path.dirname(__file__)
with open(os.path.join(currentFileDirectory, "README.md"), "r") as f:
    readme = f.read()

# Maintain dependencies of requirements.txt
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info ./**/__pycache__ ./__pycache__ ./.eggs ./.cache')

setup(
    name="modereddit",
    version="0.0.1",
    description="Modereddit - An open source system to help moderate the front page of the internet",
    long_description=readme,
    author="Alejandro Saucedo",
    author_email="a@ethical.institute",
    url="https://github.com/axsauze/modereddit",
    classifiers=[
        "Intended Audience :: Developers",
        "Natural Language :: English",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    keywords="modereddit, machine learning, deep learning, bias evaluation",
    license="MIT",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    include_package_data=True,
    install_requires=requirements,
    data_files=[ (".", ["LICENSE"]) ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    test_suite='tests',
    cmdclass={
        'clean': CleanCommand
    }
)
