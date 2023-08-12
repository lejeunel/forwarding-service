FROM python:3.8 as builder


ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.3.2  \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    curl \
    build-essential \
    gcc

# Install Poetry - respects $POETRY_VERSION & $POETRY_HOME
RUN curl -sSL https://install.python-poetry.org | python3 -

WORKDIR $PYSETUP_PATH
ADD pyproject.toml poetry.lock ./
RUN poetry install --only main


FROM python:3.8

ENV PYSETUP_PATH="/opt/pysetup" \
    POETRY_HOME="/opt/poetry" \
    VENV_PATH="/opt/pysetup/.venv" \
    POETRY_VIRTUALENVS_IN_PROJECT=1

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

# Copying poetry and venv into image
COPY --from=builder $POETRY_HOME $POETRY_HOME
COPY --from=builder $PYSETUP_PATH $PYSETUP_PATH

# venv already has runtime deps installed we get a quicker install
WORKDIR $PYSETUP_PATH
RUN poetry install

WORKDIR /cls-forwarding-service
COPY . .

EXPOSE 80
ENTRYPOINT ./docker-entrypoint.sh $0 $@
CMD ["flask", "--app", "fsapp/dev", "--debug", "run", "--host=0.0.0.0"]
