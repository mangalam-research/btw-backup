from setuptools import setup, find_packages

version = open('VERSION').read().strip()

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
        'nose>=1.3.0',
        'pytimeparse>=1.1.4,<=2',
        'pyhash>=0.6.2,<1',
    ],
    data_files=[
        ('.', ['LICENSE', 'VERSION'])
    ],
    # use_2to3=True,
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Operating System :: POSIX",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)"],
)