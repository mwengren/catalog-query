from setuptools import setup

reqs = [line.strip() for line in open('requirements.txt')]

def readme():
    with open('README.md') as f:
        return f.read()

kwargs = {
    'name': 'catalog-query',
    'author': 'Micah Wengren',
    'author_email': 'micah.wengren@gmail.com',
    'url': 'https://github.com/mwengren/catalog_query',
    'description': 'A set of functions to query the CKAN API for dataset metadata, output to local CSV files.  Queries \
        applied to IOOS Catalog by default (https://data.ioo.us/)',
    'long_description': 'readme()',
    'entry_points': {
        'console_scripts': [
            'catalog-query=catalog_query.catalog_query:main',
        ]
    },
    'classifiers': [
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: GIS'
    ],
    'packages': ['catalog_query'],
    'package_data': {

    },
    'version': '0.1.0',
}

kwargs['install_requires'] = reqs

setup(**kwargs)
