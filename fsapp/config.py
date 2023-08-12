import yaml
import logging
import os


class Config:
    """Base config"""

    TESTING = False
    DB_NAME = "file_upload"

    # API
    SWAGGER_UI_DOC_EXPANSION = "list"
    API_TITLE = "Forwarding Service API"
    API_VERSION = "v1"
    OPENAPI_VERSION = "3.0.2"
    OPENAPI_URL_PREFIX = "/api"
    OPENAPI_SWAGGER_UI_PATH = "/swagger"
    OPENAPI_SWAGGER_UI_URL = "https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.24.2/"
    OPENAPI_REDOC_PATH = "redoc"

    AUTH_MODE = 'AWS_CREDS'

    # allow source directories for uploads
    FS_ALLOWED_ROOT_DIRS = ["/fileshares"]

    RQ_REDIS_URL = "redis://redis:6379/0"

    @property
    def SQLALCHEMY_DATABASE_URI(self):
        return "sqlite:///{}.db".format(self.DB_NAME)


class ConfigTest(Config):
    DB_NAME = "file_upload_test"
    DEBUG = True
    TESTING = True


class ConfigDev(Config):
    DB_NAME = "file_upload_dev"
    DEBUG = True
    FS_ALLOWED_ROOT_DIRS = ["/"]


dev = ConfigDev()
test = ConfigTest()
prod = Config()
