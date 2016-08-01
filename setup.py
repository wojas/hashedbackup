import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="hashedbackup",
    version="0.0.1",
    author="Konrad Wojas",
    author_email="hashedbackup@m.wojas.nl",
    description="Backup files on file hash (useful for photo libraries)",
    license="MIT",
    keywords="backup hash photo",
    url="http://packages.python.org/hashedbackup",
    packages=['hashedbackup'],
    #long_description=read('README'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "Topic :: System :: Archiving :: Backup",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Operating System :: POSIX"
    ],
    entry_points={
        'console_scripts': ['hashedbackup=hashedbackup.cli:main'],
    }
)
