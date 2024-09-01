Local deployment:

Clickhouse:
https://clickhouse.com/docs/en/install#quick-install \
./clickhouse server \
./clickhouse client \
Note on creation of users: https://www.markhneedham.com/blog/2023/11/07/clickhouse-no-writeable-access-storage/

Poetry:
sudo apt update \
sudo apt install pipx \
pipx ensurepath \
sudo pipx ensurepath --global # optional to allow pipx actions with --global argument \
pipx install poetry \
-CREATE NEW TERMINAL \
poetry config virtualenvs.in-project true \
poetry shell (to activate venv) \
poetry install

Pre-commit: \
pre-commit install (for first time installation) \
pre-commit autoupdate (to bump linters)

Dagster (env vars): \
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 \
DAGSTER_HOME="/home/leo/f1/src/dagster/localhome" \
-Add a dagster.yaml file in the localhome directory to prevent concurrency with the following content:
```
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 1
```
-run bash cmd from f1 directory: dagster dev -f src/dagster/\__init__.py

For now locally to start clickhouse (from root of both f1 project and clickhouse dir): \
cp f1/data/bronze/fastf1 clickhouse/user_files -r \
cp f1/data/bronze/fantasy clickhouse/user_files -r

If fastf1 becomes very slow, need to clear cache: \
fastf1.Cache.clear_cache()

Local k8s: \
kubectl install: \
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/ linux/amd64/kubectl" \
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl \
minikube install: \
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 \
sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64 \
helm install: \
curl https://baltocdn.com/helm/signing.asc | gpg --dearmor | sudo tee /usr/share/keyrings/helm.gpg > /dev/null \
sudo apt-get install apt-transport-https --yes \
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list \
sudo apt-get update \
sudo apt-get install helm

minikube delete --all (to kill previously running clusters) \
minikube start \
helm repo add dagster https://dagster-io.github.io/helm \
helm install localname-dagster dagster/dagster --values /home/leo/Desktop/dev/f1/src/dagster/user_deployment.yaml \
kubectl port-forward service/dagster-dagster-webserver 8081:80
