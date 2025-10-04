from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

# Repo dbt à déployer (modifie l’URL)
DBT_REPO = "https://github.com/rgargour/shop_now.git"
DBT_DIR = "/opt/airflow/rgargour"
DBT_PROFILE_DIR = f"{DBT_DIR}"

default_args = {"owner": "data", "retries": 1, "retry_delay": timedelta(minutes=5)}
common_env = {
  "HOME": "/opt/airflow",                # dossier existant et inscriptible
  "DBT_PROFILES_DIR": DBT_PROFILE_DIR,
  "MOTHERDUCK_TOKEN": os.getenv("MOTHERDUCK_TOKEN",""),
}


with DAG(
    dag_id="dbt_build_motherduck_latest_tag",
    start_date=datetime(2025, 1, 1),
    schedule=None,      # déclenchement manuel pour commencer
    catchup=False,
    default_args=default_args,
    tags=["dbt", "motherduck", "prod"],
) as dag:

    prepare = BashOperator(
        task_id="prepare_workspace",
        bash_command=f"rm -rf {DBT_DIR} && mkdir -p {DBT_DIR} && mkdir -p /opt/airflow/.duckdb",
    )

    # Clone et checkout du DERNIER tag (SemVer vX.Y.Z si dispo, sinon dernier tag annoté)
    clone_checkout = BashOperator(
        task_id="clone_and_checkout_latest_tag",
        bash_command=f"""
            set -euo pipefail
            git clone --depth 1 {DBT_REPO} {DBT_DIR}
            cd {DBT_DIR}
            git fetch --tags --force
            TAG="$(git tag -l 'v*' | sort -V | tail -n1)"
            if [ -z "$TAG" ]; then TAG="$(git describe --tags --abbrev=0 2>/dev/null || true)"; fi
            test -n "$TAG" || (echo 'No tag found' && exit 1)
            git checkout "$TAG"
            echo "Checked out $TAG"
        """,
        env={"GIT_TERMINAL_PROMPT": "0"},
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"source /opt/airflow/venv/bin/activate && cd {DBT_DIR} && dbt deps --no-use-colors",
        env=common_env,
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug_prod",
        bash_command=f"source /opt/airflow/venv/bin/activate && cd {DBT_DIR} && dbt debug --target dev --no-use-colors",
        env=common_env,
    )

    dbt_build = BashOperator(
        task_id="dbt_build_prod_1",
        bash_command=f"source /opt/airflow/venv/bin/activate && cd {DBT_DIR} && dbt build --target dev --no-use-colors",
        env=common_env,
    )

    
    prepare >> clone_checkout >> dbt_deps >> dbt_debug >> dbt_build
