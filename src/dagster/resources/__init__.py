from dagster_airbyte import AirbyteResource

from dagster import EnvVar

airbyte_instance = AirbyteResource(
    host="airbyte.f1toolbox.com",
    port="",
    # only basic auth is supported as of dagster 1.8.13
    username=EnvVar("airbyte-username"),
    password=EnvVar("instance-admin-password"),
    use_https=True,
)
