from setuptools import setup

from elasticstat import __version__, __author__


def read_requirements(env_type):
    with open('requirements/{}.txt'.format(env_type), 'r') as fd:
        return [line.strip() for line in fd if not line.startswith('-') and not line.startswith('#')]


setup(
    name='elasticstat',
    version=__version__,
    description='Real-time performance monitoring of an Elasticsearch cluster from the command line',
    author=__author__,
    author_email='jtharp@objectrocket.com',
    url = 'https://github.com/objectrocket/elasticstat',
    download_url = 'https://github.com/objectrocket/elasticstat/archive/1.3.0.tar.gz',
    install_requires=read_requirements('prod'),
    packages=['elasticstat'],
    entry_points={
        'console_scripts': [
            'elasticstat = elasticstat.elasticstat:main'
        ]
    }
)
