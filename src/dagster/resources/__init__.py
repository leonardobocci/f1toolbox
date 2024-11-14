from dagster_airbyte import AirbyteResource

from src.dagster.resources.retrieve_k8s_secret import retrieve_secret

airbyte_passwords = retrieve_secret("airbyte-auth-secrets", "f1toolbox-core")
airbyte_username = retrieve_secret("airbyte-username", "f1toolbox-core")

airbyte_instance = AirbyteResource(
    host="airbyte.f1toolbox.com",
    port="",
    # only basic auth is supported as of dagster 1.8.13
    username=airbyte_username["username"],
    password=airbyte_passwords["instance-admin-password"],
    use_https=True,
)
