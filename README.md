# GraphVIPER

[![Python 3.11 3.12 3.13](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/downloads/release/python-3130/)
[![Linux Tests](https://github.com/casangi/graphviper/actions/workflows/python-testing-linux.yml/badge.svg?branch=main)](https://github.com/casangi/graphviper/actions/workflows/python-testing-linux.yml?query=branch%3Amain)
[![macOS Tests](https://github.com/casangi/graphviper/actions/workflows/python-testing-macos.yml/badge.svg?branch=main)](https://github.com/casangi/graphviper/actions/workflows/python-testing-macos.yml?query=branch%3Amain)
[![ipynb Tests](https://github.com/casangi/graphviper/actions/workflows/run-ipynb.yml/badge.svg?branch=main)](https://github.com/casangi/graphviper/actions/workflows/run-ipynb.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/casangi/graphviper/branch/main/graph/badge.svg)](https://codecov.io/gh/casangi/graphviper/branch/main/graphviper)
[![Documentation Status](https://readthedocs.org/projects/graphviper/badge/?version=latest)](https://graphviper.readthedocs.io)
[![Version Status](https://img.shields.io/pypi/v/graphviper.svg)](https://pypi.python.org/pypi/graphviper/)

GraphVIPER (Visibility and Image Parallel Execution Reduction) is a [Dask](https://docs.dask.org/) based MapReduce package. It allows for mapping a dictionary of [xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html)s to [Dask graph nodes](https://docs.dask.org/en/latest/graphs.html) followed by a reduce step.

**GraphVIPER is in development and breaking API changes will happen.**

The best place to start with GraphVIPER is doing the [graph building tutorial](https://graphviper.readthedocs.io/en/latest/graph_building_tutorial.html).

## Developer Setup

```bash
git clone git@github.com:casangi/graphviper.git
cd graphviper
pip install -e '.[all]'
pre-commit install
```

The `pre-commit install` step sets up git hooks that automatically run code
formatting and import sorting (ruff) and strip Jupyter notebook outputs
(nbstripout) on every commit. This keeps diffs clean and prevents large binary
outputs from bloating the repository. The same hooks run in CI (the
`pre-commit` workflow), so run them locally before pushing.

If `pre-commit` detects and makes any changes, those files will need to be
re-staged before committing. This is to allow the developer to inspect the
modified file before committing.
