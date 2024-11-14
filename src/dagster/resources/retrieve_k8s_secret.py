from base64 import b64decode

from kubernetes import client, config


def retrieve_secret(secret_name: str, namespace: str) -> dict:
    try:
        config.load_incluster_config()  # inside a Kubernetes cluster
    except config.ConfigException:
        config.load_kube_config()  # locally

    v1 = client.CoreV1Api()

    secret = v1.read_namespaced_secret(secret_name, namespace)
    # Decode
    decoded_secret = {}
    for key, value in secret.data.items():
        decoded_secret[key] = b64decode(value).decode("utf-8")
    return decoded_secret
