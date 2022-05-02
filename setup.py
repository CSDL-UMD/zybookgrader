""" Setuptools script """

from setuptools import setup, find_packages

kwargs = dict(
    name="zybookgrader",
    description='MSP grader for ZyBook assignment',
    author='Giovanni Luca Ciampaglia',
    author_email='glc3@mail.usf.edu',
    license='MIT',
    url='',
    packages=find_packages(),
    install_requires=[
        'pandas>=0.22',
        'dateutil'
    ],
    entry_points={
        'console_scripts': [
            "zybookgrader = zybookgrader.main:main"
        ]
    }
)

if __name__ == '__main__':
    setup(**kwargs)
