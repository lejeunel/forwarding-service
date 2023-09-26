# IAM credentials sharing

## Setup AWS IAM credentials automated rotation

Launch **smol-iam-automation** service catalog product to:
- Create IAM user
- Create IAM role to authenticate to vault
- Create a Lambda function that automatically refresh credentials at set intervals
- Pushes new credentials to Vault under specified path

Requirement:
- Create a new group in Fort-know, e.g. my-group
- Allow IAM role above to authenticate to vault and push credentials

## Retrieve stored credentials from VM
- Add a new AppRole, e.g. **my-approle** in Fort-Knox and specify vault path where credentials are stored if necessary
- This will create a new **role-id**
- Login from corporate device as usual with
```console
vault login --method=oidc
```
- Retrieve role-id that corresponds with AppRole with
```console
vault read auth/approle/role/my-approle/role-id
```
- Auto-generate secret-id and retrieve it with
```console
vault write -f auth/approle/role/my-approle/secret-id
```

## Use vault's Python client to read vault path
Use [hvac](https://pypi.org/project/hvac/) to authenticate to vault
via Python
