from setuptools import setup

from elasticstat import __version__, __author__

setup(
    name='elasticstat',
    version=__version__,
    description='Elasticsearch cluster performance overview tool',
    author=__author__,
    author_email='jeff.tharp@rackspace.com',
    install_requires=['elasticsearch'],
    packages=['elasticstat'],
    entry_points={
        'console_scripts': [
            'elasticstat = elasticstat.elasticstat:main'
        ]
    }
)
