# Resilient data forwarding for AWS S3

## Introduction

This small app aims to allow robust and resilient data transfer to cloud storage.
As of today, we support local file-system as source, and S3 bucket as destination.

This is useful when working with large volumes of data, since we 
internally stores all data transactions in a SQLite database to allow resuming.

## Installation




## Unit testing
Install package with dev dependencies
``` sh
poetry install --with dev
```

Run tests

``` sh
poetry run pytest
```

## CLI

For showing help page
```sh
>flask --app app/dev --help
```

## SQLite 

We use a sqlite database that keep each file uploaded and with bucket and prefix used.

## Configuration file

This app need a config file (.env)

```
VAULT_ROLE_ID=XXXXXX
VAULT_SECRET_ID=XXXXXX
VAULT_URL=XXXXXX
VAULT_TOKEN_PATH=XXXXXX
AWS_USER=XXXXXX
AWS_ACCESS_KEY_ID=XXXXXX
AWS_SECRET_ACCESS_KEY=XXXXXX
```
