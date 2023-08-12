
class Config:
    """Base config"""

    TESTING = False

    FORWARDER_AUTH_MODE = 'AWS_CREDS'
    FORWARDER_SOURCE = 'filesystem'
    FORWARDER_DESTINATION = 's3'
    FORWARDER_CHECKSUM = True

    SQLALCHEMY_DATABASE_URI = "sqlite:///items.db"


class ConfigTest(Config):
    SQLALCHEMY_DATABASE_URI = "sqlite:///test.db"
    AUTH_MODE = None
    TESTING = True


test = ConfigTest()
notest = Config()
