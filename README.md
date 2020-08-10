# context_retriever

Prerequisite:
- java 8
- ElasticSearch
- python 3.7
- docker
- docker-compose

How to run
- Refer [Deploysteps](./Deploysteps.md)

## Setup and Prerequisites

Install the Python dependencies:
```
pip3 install -r requirements.txt
```

You may also need `pip install -r test-requirements.txt`. Depending on the parts of bugbug you want to run, you might need to install dependencies from other requirement files (find them with `find . -name "*requirements*"`).

Currently, Python 3.7+ is required.

### Auto-formatting

This project is using [pre-commit](https://pre-commit.com/). Please run `pre-commit install` to install the git pre-commit hooks on your clone.

Every time you will try to commit, pre-commit will run checks on your files to make sure they follow our style standards and they aren't affected by some simple issues. If the checks fail, pre-commit won't let you commit.

