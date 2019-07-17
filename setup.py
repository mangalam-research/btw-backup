from setuptools import setup, find_packages

version = open('btw_backup/VERSION').read().strip()

setup(
    name="btw-backup",
    version=version,
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'btw-backup = btw_backup.__main__:main'
        ],
    },
    author="Louis-Dominique Dubeau",
    author_email="ldd@lddubeau.com",
    description="Backup script for BTW.",
    license="MPL 2.0",
    keywords=["backup"],
    url="https://github.com/mangalam-research/btw-backup",
    install_requires=[
        'pytimeparse>=1.1.8,<=2',
        'pyhash>=0.9.3,<1',
        'pyee>=6,<7',
        'awscli>=1.16.198,<2',
        's3cmd<3',
    ],
    tests_require=[
        'psycopg2>=2.5.2,<3'
    ],
    test_suite='nose.collector',
    setup_requires=['nose>=1.3.0'],
    include_package_data=True,
    # use_2to3=True,
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Operating System :: POSIX",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)"],
)
