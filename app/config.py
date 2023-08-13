class Config:
    """Base config"""

    TESTING = False

    """
    Authentication options:
    - profile: will use credentials found in ~/.aws/credentials
    - env: will use environment variables or .env
    - vault: will fetch credentials from Vault (experimental)
    """
    UPLOADER_AUTH_MODE = "profile"
    UPLOADER_CHECKSUM = True
    UPLOADER_N_PROCS = 4

    SQLALCHEMY_DATABASE_URI = "sqlite:///upload.db"


class ConfigTest(Config):
    SQLALCHEMY_DATABASE_URI = "sqlite:///test.db"
    AUTH_MODE = None
    TESTING = True
    UPLOADER_N_PROCS = 1


test = ConfigTest()
notest = Config()
