# -*- coding: utf-8 -*-

from parse_to_syncano import __version__
from setuptools import find_packages, setup


def readme():
    with open('README.rst') as f:
        return f.read()

setup(
    name='parse2syncano',
    version=__version__,
    description='A Syncano tool for migrate the Parse data',
    long_description=readme(),
    author=u'Sebastian Opałczyński',
    author_email='sebastian.opalczynski@syncano.com',
    url='http://syncano.com',
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        'syncano>=5.1.0'
    ],
    entry_points="""
        [console_scripts]
        parse2syncano=parse_to_syncano.moses:parse2syncano
    """
)
