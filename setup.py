from setuptools import setup


with open('requirements.txt') as f:
    requirements = f.read().splitlines()


setup_requirements = ['pytest-runner']

test_requirements = ['pytest']


setup(
    name="beam_sink",
    author="Mitchell Lisle",
    author_email='m.lisle90@gmail.com',
    description="An Apache Beam Sink Library for Databases and other Sinks.",
    install_requires=requirements,
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/mitchelllisle/beam-sink',
    version='0.7.0',
    zip_safe=False,
)
