from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secrets import Secret  # optionnel si vous utilisez un Secret K8s

# --- Paramètres projet ---
DBT_REPO = "https://github.com/rgargour/shop_now.git"
DBT_DIR = "/workspace/rgargour"               # workspace dans le conteneur
DBT_PROFILE_DIR = DBT_DIR                     # suppose profiles.yml présent dans le repo (sinon montez-le)
NAMESPACE = os.getenv("AIRFLOW__K8S__NAMESPACE", "airflow")

# Image qui contient dbt-duckdb (modifiez selon votre registre/versions)
DBT_IMAGE = os.getenv("DBT_IMAGE", "ghcr.io/dbt-labs/dbt-duckdb:latest")

default_args = {"owner": "data", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="dbt_build_motherduck_latest_tag_kubernetes",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "motherduck", "prod"],
) as dag:

    # Option 1 (recommandé) : récupérer le token depuis un Secret K8s nommé "motherduck" (clé "token")
    md_secret = Secret(deploy_type="env", deploy_target="MOTHERDUCK_TOKEN",
                       secret="motherduck", key="token")

    # Script bash exécuté dans le pod : prépare, clone, checkout dernier tag, dbt deps/debug/build
    run_script = f"""
        set -euo pipefail

        # Si l'image n'a pas git, on tente de l'installer (ignorer les erreurs selon image)
        (command -v git >/dev/null 2>&1) || (apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/* || true)

        rm -rf {DBT_DIR} && mkdir -p {DBT_DIR}

        echo "Cloning repo..."
        git clone --depth 1 {DBT_REPO} {DBT_DIR}
        cd {DBT_DIR}

        echo "Fetching tags..."
        git fetch --tags --force

        # Dernier tag 'vX.Y.Z' si dispo, sinon dernier tag annoté
        TAG="$(git tag -l 'v*' | sort -V | tail -n1)"
        if [ -z "$TAG" ]; then TAG="$(git describe --tags --abbrev=0 2>/dev/null || true)"; fi
        test -n "$TAG" || (echo 'No tag found' && exit 1)
        git checkout "$TAG"
        echo "Checked out $TAG"

        echo "dbt deps..."
        dbt deps --no-use-colors

        echo "dbt debug (dev)..."
        dbt debug --target dev --no-use-colors

        echo "dbt build (dev)..."
        dbt build --target dev --no-use-colors
    """

    dbt_build_pod = KubernetesPodOperator(
        task_id="dbt_build_in_pod",
        name="dbt-build-motherduck",
        namespace=NAMESPACE,
        image=DBT_IMAGE,
        # Si votre image n'a pas bash, remplacez par sh -lc
        cmds=["/bin/bash", "-lc"],
        arguments=[run_script],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        env_vars={
            "DBT_PROFILES_DIR": DBT_PROFILE_DIR,
            # Option 2 (simple) : passer le token depuis les Variables Airflow/env du scheduler
            # "MOTHERDUCK_TOKEN": os.getenv("MOTHERDUCK_TOKEN", ""),
        },
        secrets=[md_secret],  # commentez cette ligne si vous n'utilisez pas de Secret K8s
        # resources={"request_memory": "1Gi", "request_cpu": "500m"}  # ajustez au besoin
        image_pull_policy="IfNotPresent",
    )
