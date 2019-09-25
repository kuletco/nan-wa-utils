import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="nan-wa-utils",
    version="0.1.1",
    author="nan",
    author_email="nan-gameware@users.noreply.github.com",
    description="A small example package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nan-gameware/nan-wa-utils",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    python_requires='>=3.7',
    install_requires=[
        'pandas',
        'pyyaml',
        'requests',
        'jinja2',
    ],
    entry_points={
        "console_scripts": [
            "wowdb-query=wowdb.cli:run"
        ]
    },
)