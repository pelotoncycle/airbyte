#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import os

import boto3
import pytest
import yaml
from moto import mock_secretsmanager
from octavia_cli.apply import yaml_loaders


@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="module")
def secretsmanager_secrets():
    return {
        "MY_SECRET_PASSWORD": "Secret From SecretsManager",
        "MY_NESTED_SECRET": {
            "NESTED_STRUCT": {"VERY_NESTED_VALUE": "FOUND NESTED VALUE", "ANOTHER_VERY_NESTED_VALUE": "FOUND ANOTHER NESTED VALUE"}
        },
    }


@pytest.fixture(scope="module")
def secretsmanager_client(aws_credentials, secretsmanager_secrets):
    with mock_secretsmanager():
        conn = boto3.client("secretsmanager", region_name="us-east-1")
        for secret_key, secret_value in secretsmanager_secrets.items():
            if isinstance(secret_value, str):
                conn.create_secret(Name=secret_key, SecretString=secret_value)
            else:
                conn.create_secret(Name=secret_key, SecretString=json.dumps(secret_value))
        yield conn


def test_env_var_replacer(mocker):
    mocker.patch.object(yaml_loaders, "os")
    mock_node = mocker.Mock()
    assert yaml_loaders.env_var_replacer(mocker.Mock(), mock_node) == yaml_loaders.os.path.expandvars.return_value
    yaml_loaders.os.path.expandvars.assert_called_with(mock_node.value)


@pytest.fixture
def test_env_vars():
    old_environ = dict(os.environ)
    secret_env_vars = {"MY_SECRET_PASSWORD": "🤫", "ANOTHER_SECRET_VALUE": "🔒"}
    os.environ.update(secret_env_vars)
    yield secret_env_vars
    os.environ.clear()
    os.environ.update(old_environ)


def test_env_var_loader(test_env_vars, secretsmanager_client, secretsmanager_secrets):
    assert yaml_loaders.EnvVarLoader.yaml_implicit_resolvers[None] == [
        ("!environment_variable", yaml_loaders.ENV_VAR_MATCHER_PATTERN),
        ("!secretsmanager_secret", yaml_loaders.SECRETS_MANAGER_MATCHER_PATTERN),
    ]

    assert yaml_loaders.EnvVarLoader.yaml_constructors["!environment_variable"] == yaml_loaders.env_var_replacer
    assert yaml_loaders.EnvVarLoader.yaml_constructors["!secretsmanager_secret"] == yaml_loaders.SecretsManagerSecretConstructor()
    test_yaml = """
    my_secret_password: ${MY_SECRET_PASSWORD}
    another_secret_value: ${ANOTHER_SECRET_VALUE}
    my_secret_password_2: ${secretsmanager:MY_SECRET_PASSWORD}
    my_nested_secret: ${secretsmanager:MY_NESTED_SECRET.NESTED_STRUCT.VERY_NESTED_VALUE}
    another_nested_secret: ${secretsmanager:MY_NESTED_SECRET.NESTED_STRUCT.ANOTHER_VERY_NESTED_VALUE}
    """
    deserialized = yaml.load(test_yaml, yaml_loaders.EnvVarLoader)
    print(test_env_vars)
    assert deserialized == {
        "my_secret_password": test_env_vars["MY_SECRET_PASSWORD"],
        "another_secret_value": test_env_vars["ANOTHER_SECRET_VALUE"],
        "my_secret_password_2": secretsmanager_secrets["MY_SECRET_PASSWORD"],
        "my_nested_secret": secretsmanager_secrets["MY_NESTED_SECRET"]["NESTED_STRUCT"]["VERY_NESTED_VALUE"],
        "another_nested_secret": secretsmanager_secrets["MY_NESTED_SECRET"]["NESTED_STRUCT"]["ANOTHER_VERY_NESTED_VALUE"],
    }
