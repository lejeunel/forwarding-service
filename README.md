# cls-forwarding-service

## Introduction

This small app aim to upload file from a (remote) directory to a AWS S3 bucket

## Usage
The stack is configured through docker-compose and can be started from project root:

``` sh
docker-compose up
```

Start a new upload job
``` sh
flask --app fsapp/test test-upload-dir --help
```

Get information on queues
``` sh
flask --app fsapp/test rq
```

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
>flask --app fsapp/dev --help
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
