import hvac


class BaseAuthenticator:
    def __call__(self):
        pass


class S3StaticCredentials(BaseAuthenticator):
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def __call__(self):
        return {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
        }


class VaultCredentials(BaseAuthenticator):
    """
    Retrieves AWS secrets from vault safe
    """

    def __init__(self, url=None, token_path=None, role_id=None, secret_id=None) -> None:
        """
        :param url: URL of Vault safe
        :param token_path: Path where token is stored
        :param role_id: ID of AppRole
        :param secret_id: Secret ID to read path
        """
        self.url = url
        self.token_path = token_path
        self.role_id = role_id
        self.secret_id = secret_id
        self.client = hvac.Client(url=url)

    def __call__(self):
        self.client.auth.approle.login(
            role_id=self.role_id,
            secret_id=self.secret_id,
        )

        return self.client.read(self.token_path)
