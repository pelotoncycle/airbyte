#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import os
import re
from typing import Any

import boto3
import yaml

ENV_VAR_MATCHER_PATTERN = re.compile(r".*\$\{(?<!(secretsmanager:))([^}^:^{]+)\}.*")
SECRETS_MANAGER_MATCHER_PATTERN = re.compile(r".*\$\{secretsmanager:([^}^{]+)\}.*")


def env_var_replacer(loader: yaml.Loader, node: yaml.Node) -> Any:
    """Convert a YAML node to a Python object, expanding variable.

    Args:
        loader (yaml.Loader): Not used
        node (yaml.Node): Yaml node to convert to python object

    Returns:
        Any: Python object with expanded vars.
    """
    print(f"{node.value} from env")
    return os.path.expandvars(node.value)


class Singleton(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "instance"):
            cls.instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
        return cls.instance


class SecretsManagerSecretConstructor(Singleton):
    def __init__(self):
        region = os.environ.get("AWS_REGION", "us-east-1")
        self.client = boto3.client("secretsmanager", region)
        self._cache = dict()

    @staticmethod
    def parse_response(response):
        secret_string = response["SecretString"]
        if secret_string.strip().startswith("{"):
            return json.loads(secret_string)
        else:
            return secret_string

    def __call__(self, loader: yaml.Loader, node: yaml.Node) -> Any:
        """Convert a YAML node to a Python object, expanding variable.

        Args:
            loader (yaml.Loader): Not used
            node (yaml.Node): Yaml node to convert to python object

        Returns:
            Any: Python object with expanded vars.
        """
        key_pattern = re.compile("\\$\\{secretsmanager:(?P<key>[^}^\\.]+)(?P<path>.*)?\\}")
        grps = key_pattern.match(node.value).groupdict()
        secret_key = grps["key"]
        path = grps["path"].strip(".")
        if secret_key not in self._cache:
            secretsmanager_response = self.client.get_secret_value(SecretId=secret_key)
            self._cache[secret_key] = self.parse_response(secretsmanager_response)
        if path:
            sub_keys = path.split(".")
            value = self._cache[secret_key]
            for sub_key in sub_keys:
                value = value[sub_key]
            return value
        return self._cache[secret_key]


class EnvVarLoader(yaml.SafeLoader):
    pass


# All yaml nodes matching the regex will be tagged as !environment_variable.
EnvVarLoader.add_implicit_resolver("!environment_variable", ENV_VAR_MATCHER_PATTERN, None)

# All yaml nodes tagged as !environment_variable will be constructed with the env_var_replacer callback.
EnvVarLoader.add_constructor("!environment_variable", env_var_replacer)

EnvVarLoader.add_implicit_resolver("!secretsmanager_secret", SECRETS_MANAGER_MATCHER_PATTERN, None)
EnvVarLoader.add_constructor("!secretsmanager_secret", SecretsManagerSecretConstructor())
