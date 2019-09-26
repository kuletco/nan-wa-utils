import setuptools


def slurp(file_name):
    with open(file_name, "r") as f:
        return f.read()

setuptools.setup(
    name="nan-wa-utils",
    version="0.1.1",
    author="nan",
    author_email="nan-gameware@users.noreply.github.com",
    description="A small example package",
    long_description=slurp("README.md"),
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
        'jinja2',
        'pandas',
        'pyyaml',
        'requests',
        'tabulate',
    ],
    entry_points={
        "console_scripts": [
            "wowdb-query=wowdb.cli:run"
        ]
    },
)
