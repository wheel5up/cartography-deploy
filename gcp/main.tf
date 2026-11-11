locals {
  # APIs are billed/quota'd against the SA's host project, so they're enabled here only.
  apis = [
    "cloudresourcemanager.googleapis.com",
    "serviceusage.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com", # WIF -> SA impersonation
    "sts.googleapis.com",            # WIF token exchange
    "compute.googleapis.com",
    "storage.googleapis.com",
    "container.googleapis.com",
    "dns.googleapis.com",
    "cloudkms.googleapis.com",
    "bigtableadmin.googleapis.com",
    "sqladmin.googleapis.com",
    "bigquery.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "cloudfunctions.googleapis.com",
    "secretmanager.googleapis.com",
    "artifactregistry.googleapis.com",
    "run.googleapis.com",
    "aiplatform.googleapis.com",
    "notebooks.googleapis.com",
    "cloudasset.googleapis.com",
    "apikeys.googleapis.com",
  ]

  org_role_grants = {
    for pair in setproduct(keys(var.scanned_org_ids), var.org_roles) :
    "${pair[0]}|${pair[1]}" => { org_id = var.scanned_org_ids[pair[0]], role = pair[1] }
  }

  lambda_assumed_role = "arn:aws:sts::${var.aws_account_id}:assumed-role/${var.aws_lambda_role_name}"
}

# --- Host project -----------------------------------------------------------

resource "google_project" "cartography" {
  project_id      = var.project_id
  name            = "Cartography"
  org_id          = var.host_org_id
  billing_account = var.billing_account
  deletion_policy = "PREVENT"
}

resource "google_project_service" "apis" {
  for_each           = toset(local.apis)
  project            = google_project.cartography.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "tfstate" {
  project                     = google_project.cartography.project_id
  name                        = "${var.project_id}-tfstate"
  location                    = "US"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  versioning { enabled = true }
  depends_on = [google_project_service.apis]
}

# --- Service account --------------------------------------------------------

resource "google_service_account" "cartography" {
  project      = google_project.cartography.project_id
  account_id   = "cartography-reader"
  display_name = "Cartography read-only inventory"
  depends_on   = [google_project_service.apis]
}

resource "google_organization_iam_member" "reader" {
  for_each = local.org_role_grants
  org_id   = each.value.org_id
  role     = each.value.role
  member   = "serviceAccount:${google_service_account.cartography.email}"
}

# No predefined role lists BigQuery connections without also granting `use`.
# Custom org roles are per-org, so one is created in each scanned org.
resource "google_organization_iam_custom_role" "bq_connection_viewer" {
  for_each    = var.scanned_org_ids
  org_id      = each.value
  role_id     = "cartographyBigQueryConnectionViewer"
  title       = "Cartography BigQuery Connection Viewer"
  description = "List/get BigQuery connections metadata. No use."
  permissions = [
    "bigquery.connections.list",
    "bigquery.connections.get",
  ]
}

resource "google_organization_iam_member" "bq_connection_viewer" {
  for_each = var.scanned_org_ids
  org_id   = each.value
  role     = google_organization_iam_custom_role.bq_connection_viewer[each.key].name
  member   = "serviceAccount:${google_service_account.cartography.email}"
}

# --- Workload Identity Federation (AWS Lambda -> SA, no keys) ---------------

resource "google_iam_workload_identity_pool" "aws" {
  project                   = google_project.cartography.project_id
  workload_identity_pool_id = "aws-cartography"
  display_name              = "AWS cartography Lambdas"
  depends_on                = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "aws" {
  project                            = google_project.cartography.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.aws.workload_identity_pool_id
  workload_identity_pool_provider_id = "aws-${var.aws_account_id}"
  display_name                       = "AWS ${var.aws_account_id}"

  aws {
    account_id = var.aws_account_id
  }

  # Default AWS mapping: aws_role = arn:aws:sts::ACCT:assumed-role/ROLE (session name stripped)
  attribute_mapping = {
    "google.subject"     = "assertion.arn"
    "attribute.aws_role" = "assertion.arn.contains('assumed-role') ? assertion.arn.extract('{account_arn}assumed-role/') + 'assumed-role/' + assertion.arn.extract('assumed-role/{role_name}/') : assertion.arn"
  }

  # Only the loader's Lambda role can exchange tokens at all.
  attribute_condition = "attribute.aws_role == '${local.lambda_assumed_role}'"
}

resource "google_service_account_iam_member" "wif" {
  service_account_id = google_service_account.cartography.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.aws.name}/attribute.aws_role/${local.lambda_assumed_role}"
}

# Local testing: lets listed humans mint short-lived tokens as the SA.
resource "google_service_account_iam_member" "impersonators" {
  for_each           = toset(var.impersonators)
  service_account_id = google_service_account.cartography.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value
}
