
class Config:
    """Base config"""

    TESTING = False

    """
    Authentication options:
    - profile: will use credentials found in ~/.aws/credentials
    - env: will use environment variables or .env
    - vault: will fetch credentials from Vault (not finished)
    """
    FORWARDER_AUTH_MODE = 'profile'
    FORWARDER_AUTH_PROFILE = 'default'
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
