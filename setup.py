from setuptools import setup

from elasticstat import __version__, __author__

setup(
    name='elasticstat',
    version=__version__,
    description='Real-time performance monitoring of an Elasticsearch cluster from the command line',
    author=__author__,
    author_email='jtharp@objectrocket.com',
    url = 'https://github.com/objectrocket/elasticstat',
    download_url = 'https://github.com/objectrocket/elasticstat/archive/1.2.0.tar.gz',
    install_requires=['elasticsearch'],
    packages=['elasticstat'],
    entry_points={
        'console_scripts': [
            'elasticstat = elasticstat.elasticstat:main'
        ]
    }
)
