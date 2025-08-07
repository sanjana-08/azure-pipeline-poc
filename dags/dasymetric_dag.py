from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.sensors.python import PythonSensor

# Adjust these paths to your local setup
KIND_CONFIG = "/home/sanjana/daysymmetric_mapping/k8s/kind-config.yaml"

K8S_DIR      = "/home/sanjana/daysymmetric_mapping/k8s"

default_args = {
    "owner": "sanjana",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dasymetric_kind_workflow",
    default_args=default_args,
    start_date=datetime(2025, 8, 4),
    schedule_interval=None,
    catchup=False,
    tags=["dasymetric", "kind", "kubernetes"],
) as dag:

    # 1️⃣ Create Kind cluster
    create_cluster = BashOperator(
        task_id="kind_create_cluster",
        bash_command=(
            "kind delete cluster --name dasymetric-kind || true && "
            "kind create cluster --name dasymetric-kind --config {{ params.kind_config }} "
        ),
        params={"kind_config": KIND_CONFIG},
    )

    # 2️⃣ Build the Docker image
    build_image = BashOperator(
        task_id="docker_build",
        bash_command=(
            "docker build -t dasymetric:latest /home/sanjana/daysymmetric_mapping"
        ),
    )

    # 3️⃣ Load the image into Kind
    load_image = BashOperator(
        task_id="kind_load_image",
        bash_command=(
            "kind load docker-image dasymetric:latest --name dasymetric-kind"
        ),
    )

    # 4️⃣ Deploy Kubernetes manifests
    deploy_k8s = BashOperator(
        task_id="kubectl_apply",
        bash_command=(
            "kubectl apply -f {{ params.k8s_dir }}/scheduler-deployment.yaml && "
            "kubectl apply -f {{ params.k8s_dir }}/scheduler-service.yaml && "
            "kubectl apply -f {{ params.k8s_dir }}/worker-deployment.yaml && "
            "kubectl apply -f {{ params.k8s_dir }}/dasymetric-job.yaml"
        ),
        params={"k8s_dir": K8S_DIR},
    )

    # 5️⃣ Sensor: wait until the Job pod is complete
    def job_succeeded():
        import subprocess, json
        out = subprocess.check_output([
            "kubectl", "get", "job", "dasymetric-run",
            "-o", "json"
        ])
        status = json.loads(out.decode())
        succ = status.get("status", {}).get("succeeded", 0)
        return succ >= 1

    wait_for_job = PythonSensor(
        task_id="wait_for_dasymetric_job",
        python_callable=job_succeeded,
        poke_interval=15,
        timeout=60*20,  # wait up to 20 minutes
    )

    # 6️⃣ (Optional) Tear down the Kind cluster
    delete_cluster = BashOperator(
        task_id="kind_delete_cluster",
        bash_command="kind delete cluster --name dasymetric-kind",
        trigger_rule="all_done",  # run even if upstream fails
    )

    # Define task order
    create_cluster >> build_image >> load_image >> deploy_k8s >> wait_for_job >> delete_cluster
