"""
Cartography Kubernetes (EKS) sync — Lambda handler.

One invocation = one EKS cluster, with no AWS CLI and no ambient credentials:

  1. Parse region / account / name out of the cluster ARN.
  2. Assume arn:aws:iam::<account>:role/cartography-k8s-reader (boto3).
     If INTERMEDIATE_ROLE_ARN is set, hop through it first (for reader roles
     that trust that role instead of this function's role).
  3. eks:DescribeCluster -> endpoint + CA.
  4. Mint the EKS bearer token in-process (presigned STS GetCallerIdentity —
     exactly what `aws eks get-token` does).
  5. Write a static-token kubeconfig to /tmp. Context / cluster / user are all
     named with the cluster ARN, matching what `aws eks update-kubeconfig`
     produces, so KubernetesCluster.name / external_id match a normal
     cartography CLI run.
  6. Run the same sync_* sequence as
     cartography.intel.kubernetes.start_k8s_ingestion(), with two differences:
     secrets are synced before pods (see _run_k8s_sync), and sync_eks() is
     handed the assumed-role boto3 session explicitly. Upstream builds a bare boto3.Session() there, which in Lambda
     would resolve to this function's own role, not the reader role.
  7. Emit SyncDuration / SyncSuccess (Namespace=Cartography,
     Module=k8s, Cluster=<name>).

Event:    {"cluster_arn": "arn:aws:eks:<region>:<account>:cluster/<name>"}
Response: {"status": "SUCCEEDED|FAILED", "clusterArn", "cluster",
           "durationSeconds", "updateTag", "error"?}

Each cluster gets its own update_tag. Every cleanup in the k8s module is scoped by CLUSTER_ID,
so per-cluster tags are safe.
"""

import base64
import logging
import os
import re
import time
from typing import Any

import boto3
import neo4j
import yaml
from botocore.signers import RequestSigner

from cartography.analysis.kubernetes.analysis import K8S_COMPUTE_ASSET_EXPOSURE_JOBS
from cartography.analysis.kubernetes.analysis import K8S_LB_EXPOSURE_JOBS
from cartography.intel.kubernetes.clusters import sync_kubernetes_cluster
from cartography.intel.kubernetes.eks import sync as sync_eks
from cartography.intel.kubernetes.gateway_api import sync_gateway_api
from cartography.intel.kubernetes.ingress import sync_ingress
from cartography.intel.kubernetes.namespaces import sync_namespaces
from cartography.intel.kubernetes.networkpolicies import sync_network_policies
from cartography.intel.kubernetes.nodes import sync_nodes
from cartography.intel.kubernetes.pods import sync_pods
from cartography.intel.kubernetes.rbac import sync_kubernetes_rbac
from cartography.intel.kubernetes.secrets import sync_secrets
from cartography.intel.kubernetes.services import sync_services
from cartography.intel.kubernetes.storage import sync_storage
from cartography.intel.kubernetes.util import get_k8s_clients
from cartography.intel.kubernetes.workloads import sync_workloads
from cartography.util import run_typed_analysis_job

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

CLUSTER_ARN_RE = re.compile(r"^arn:aws:eks:([a-z0-9-]+):(\d{12}):cluster/(.+)$")

READER_ROLE_NAME = os.environ.get("READER_ROLE_NAME", "cartography-k8s-reader")
ASSUME_ROLE_DURATION = int(os.environ.get("ASSUME_ROLE_DURATION_SECONDS", "900"))
# Interim hop (until cartography-k8s-reader trusts this Lambda's role directly):
# assume this role first, then the reader from it. Empty = assume the reader
# directly with the Lambda's own credentials.
INTERMEDIATE_ROLE_ARN = os.environ.get("INTERMEDIATE_ROLE_ARN", "")
# Distinct session name so CloudTrail can attribute these calls to this Lambda
# even when they arrive via cartography-loader-role.
ROLE_SESSION_NAME = "cartography-k8s-lambda"
METRICS_REGION = os.environ.get("METRICS_REGION", os.environ.get("AWS_REGION"))
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "Cartography")

# EKS token prefix / header, per aws-iam-authenticator and `aws eks get-token`.
_TOKEN_PREFIX = "k8s-aws-v1."
_CLUSTER_ID_HEADER = "x-k8s-aws-id"
_TOKEN_PRESIGN_SECONDS = 60  # same value the AWS CLI uses


def _parse_cluster_arn(arn: str) -> tuple[str, str, str]:
    m = CLUSTER_ARN_RE.match(arn or "")
    if not m:
        raise ValueError(
            f"'{arn}' is not a valid EKS cluster ARN "
            "(expected arn:aws:eks:<region>:<account-id>:cluster/<name>)"
        )
    return m.group(1), m.group(2), m.group(3)


def _assume(base: boto3.Session, role_arn: str, region: str) -> boto3.Session:
    logger.info("Assuming %s", role_arn)
    creds = base.client(
        "sts", region_name=region, endpoint_url=f"https://sts.{region}.amazonaws.com"
    ).assume_role(
        RoleArn=role_arn,
        RoleSessionName=ROLE_SESSION_NAME,
        # Chained sessions are capped at 1h by STS; 900s is well under that.
        DurationSeconds=ASSUME_ROLE_DURATION,
    )["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )


def _assume_reader(account_id: str, region: str) -> boto3.Session:
    base = boto3.Session()
    if INTERMEDIATE_ROLE_ARN:
        base = _assume(base, INTERMEDIATE_ROLE_ARN, region)
    return _assume(base, f"arn:aws:iam::{account_id}:role/{READER_ROLE_NAME}", region)


def _eks_token(session: boto3.Session, cluster_name: str, region: str) -> str:
    """Bearer token for the EKS API server: a presigned STS GetCallerIdentity
    URL carrying the x-k8s-aws-id header, base64url-encoded. Identical to
    `aws eks get-token`. The API server accepts it for 15 minutes; a sync
    takes well under a minute and the Lambda itself is capped at 15."""
    sts = session.client("sts", region_name=region)
    signer = RequestSigner(
        sts.meta.service_model.service_id,
        region,
        "sts",
        "v4",
        session.get_credentials(),
        session.events,
    )
    url = signer.generate_presigned_url(
        {
            "method": "GET",
            "url": f"https://sts.{region}.amazonaws.com/"
            "?Action=GetCallerIdentity&Version=2011-06-15",
            "body": {},
            "headers": {_CLUSTER_ID_HEADER: cluster_name},
            "context": {},
        },
        region_name=region,
        expires_in=_TOKEN_PRESIGN_SECONDS,
        operation_name="",
    )
    return _TOKEN_PREFIX + base64.urlsafe_b64encode(url.encode()).decode().rstrip("=")


def _write_kubeconfig(
    cluster_arn: str, endpoint: str, ca_data: str, token: str, path: str
) -> None:
    kubeconfig = {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [
            {
                "name": cluster_arn,
                "cluster": {
                    "server": endpoint,
                    "certificate-authority-data": ca_data,
                },
            }
        ],
        "users": [{"name": cluster_arn, "user": {"token": token}}],
        "contexts": [
            {
                "name": cluster_arn,
                "context": {"cluster": cluster_arn, "user": cluster_arn},
            }
        ],
        "current-context": cluster_arn,
    }
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        yaml.safe_dump(kubeconfig, f)


def _run_k8s_sync(
    session: neo4j.Session,
    kubeconfig_path: str,
    aws_session: boto3.Session,
    update_tag: int,
) -> None:
    """Same sequence as start_k8s_ingestion() (pinned fork tag), EKS branch
    only, with the assumed-role session passed to sync_eks()."""
    common_job_parameters: dict[str, Any] = {"UPDATE_TAG": update_tag}

    clients = get_k8s_clients(kubeconfig_path)
    if len(clients) != 1:
        raise RuntimeError(f"Expected exactly 1 kubeconfig context, got {len(clients)}")
    client = clients[0]

    cluster_info = sync_kubernetes_cluster(
        session, client, update_tag, common_job_parameters
    )
    common_job_parameters["CLUSTER_ID"] = cluster_info.get("id")
    cluster_external_ref = cluster_info.get("external_id") or cluster_info.get(
        "name", ""
    )
    region, _, _ = _parse_cluster_arn(cluster_external_ref)

    sync_namespaces(session, client, update_tag, common_job_parameters)
    node_arch_map = sync_nodes(session, client, update_tag, common_job_parameters)
    sync_kubernetes_rbac(session, client, update_tag, common_job_parameters)
    replicaset_owner_map = sync_workloads(
        session, client, update_tag, common_job_parameters
    )
    sync_storage(session, client, update_tag, common_job_parameters)
    sync_eks(
        session,
        client,
        aws_session,
        region,
        update_tag,
        cluster_info.get("id", ""),
        cluster_external_ref,
    )
    # Deliberate deviation from upstream order (which syncs pods first):
    # pod -> secret edges (USES_SECRET*, matched on KubernetesSecret.composite_id)
    # are only written if the secret node already exists. Upstream order drops
    # the edge for any secret created/recreated since the previous run (e.g.
    # ExternalSecret PreSync hooks on every Argo deploy). Nothing in sync_secrets
    # depends on pods, so loading secrets first is safe.
    sync_secrets(session, client, update_tag, common_job_parameters)
    all_pods = sync_pods(
        session,
        client,
        update_tag,
        common_job_parameters,
        region=region,
        node_arch_map=node_arch_map,
        replicaset_owner_map=replicaset_owner_map,
    )
    sync_services(session, client, all_pods, update_tag, common_job_parameters)
    sync_network_policies(session, client, all_pods, update_tag, common_job_parameters)
    sync_gateway_api(session, client, update_tag, common_job_parameters)
    sync_ingress(session, client, update_tag, common_job_parameters)

    for job in K8S_COMPUTE_ASSET_EXPOSURE_JOBS:
        run_typed_analysis_job(job, session, common_job_parameters)
    for job in K8S_LB_EXPOSURE_JOBS:
        run_typed_analysis_job(job, session, common_job_parameters)


def _put_metrics(cluster_name: str, duration: int, success: bool) -> None:
    try:
        dims = [
            {"Name": "Module", "Value": "k8s"},
            {"Name": "Cluster", "Value": cluster_name},
        ]
        boto3.client("cloudwatch", region_name=METRICS_REGION).put_metric_data(
            Namespace=METRICS_NAMESPACE,
            MetricData=[
                {
                    "MetricName": "SyncDuration",
                    "Dimensions": dims,
                    "Value": duration,
                    "Unit": "Seconds",
                },
                {
                    "MetricName": "SyncSuccess",
                    "Dimensions": dims,
                    "Value": 1 if success else 0,
                    "Unit": "Count",
                },
            ],
        )
    except Exception:
        logger.exception("Failed to put CloudWatch metrics for %s", cluster_name)


def _get_neo4j_password() -> str:
    """Neo4j password from SSM at invoke time, never a Lambda env var.
    NEO4J_PASSWORD_SSM_PARAM is the parameter NAME only."""
    response = boto3.client("ssm").get_parameter(
        Name=os.environ.get(
            "NEO4J_PASSWORD_SSM_PARAM", "/cartography/neo4j/password"
        ),
        WithDecryption=True,
    )
    return response["Parameter"]["Value"]


def _neo4j_driver() -> neo4j.Driver:
    return neo4j.GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=("neo4j", _get_neo4j_password()),
        max_connection_lifetime=int(os.environ.get("NEO4J_MAX_CONNECTION_LIFETIME", "200")),
    )


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    cluster_arn = (event or {}).get("cluster_arn", "")
    start = time.time()
    update_tag = int(start)
    result: dict[str, Any] = {"clusterArn": cluster_arn, "updateTag": update_tag}
    cluster_name = cluster_arn.rsplit("/", 1)[-1] if cluster_arn else "unknown"
    kubeconfig_path = ""

    try:
        region, account_id, cluster_name = _parse_cluster_arn(cluster_arn)
        result["cluster"] = cluster_name

        aws_session = _assume_reader(account_id, region)
        cluster = aws_session.client("eks").describe_cluster(name=cluster_name)["cluster"]
        token = _eks_token(aws_session, cluster_name, region)

        kubeconfig_path = f"/tmp/kubeconfig-{account_id}-{cluster_name}.yaml"
        _write_kubeconfig(
            cluster_arn,
            cluster["endpoint"],
            cluster["certificateAuthority"]["data"],
            token,
            kubeconfig_path,
        )

        logger.info("Starting k8s sync for %s (update_tag=%s)", cluster_arn, update_tag)
        driver = _neo4j_driver()
        try:
            with driver.session() as session:
                _run_k8s_sync(session, kubeconfig_path, aws_session, update_tag)
        finally:
            driver.close()

        result["status"] = "SUCCEEDED"
    except Exception as e:
        logger.exception("k8s sync failed for %s", cluster_arn)
        result["status"] = "FAILED"
        result["error"] = f"{type(e).__name__}: {e}"
    finally:
        if kubeconfig_path:
            try:
                os.remove(kubeconfig_path)
            except FileNotFoundError:
                pass

    duration = int(time.time() - start)
    result["durationSeconds"] = duration
    _put_metrics(cluster_name, duration, result["status"] == "SUCCEEDED")
    logger.info(
        "Completed k8s sync for %s: %s in %ss", cluster_arn, result["status"], duration
    )
    return result
