from setuptools import setup, find_packages


with open('requirements.txt') as f:
    requirements = f.read().splitlines()


setup_requirements = ['pytest-runner']

test_requirements = ['pytest']


setup(
    name='beam_sink',
    author='Mitchell Lisle',
    author_email='m.lisle90@gmail.com',
    description="An Apache Beam Sink Library for Databases and other Sinks.",
    install_requires=requirements,
    packages=find_packages(),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/mitchelllisle/beam-sink',
    version='0.9.0',
    zip_safe=False,
)
