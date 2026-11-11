variable "project_id" {
  description = "Host project for the Cartography service account and WIF pool."
  type        = string
}

variable "host_org_id" {
  description = "Org that owns the host project."
  type        = string
}

variable "billing_account" {
  description = "Billing account ID (XXXXXX-XXXXXX-XXXXXX) for the host project."
  type        = string
}

variable "scanned_org_ids" {
  description = "Orgs Cartography inventories. The SA gets org-level read roles in each."
  type        = map(string)
}

variable "aws_account_id" {
  description = "AWS account the cartography Lambdas run in."
  type        = string
}

variable "aws_lambda_role_name" {
  description = "IAM role of the GCP loader Lambda: cartography-gcp-lambda-loader-role-<region> (created by gcp-lambda-loader/template.yaml)."
  type        = string
}

variable "org_roles" {
  description = "Read-only roles granted to the SA at each scanned org."
  type        = list(string)
  default = [
    # Required by cartography
    "roles/iam.securityReviewer",
    "roles/compute.viewer",
    "roles/resourcemanager.organizationViewer",
    "roles/resourcemanager.folderViewer",
    # Optional, metadata-only
    "roles/cloudasset.viewer",
    "roles/run.viewer",
    "roles/notebooks.viewer",
    "roles/serviceusage.apiKeysViewer",
    "roles/bigquery.metadataViewer", # datasets/tables/routines, no table data
    # BigQuery connections: custom role "cartographyBqConnectionMetadataViewer" (main.tf)
    # Deliberately omitted (grant data/usage access, not just metadata):
    #   roles/bigquery.dataViewer      - can read table contents
    #   roles/bigquery.connectionUser  - includes bigquery.connections.use
    #   roles/artifactregistry.reader  - can pull artifacts
  ]
}

variable "impersonators" {
  description = "Principals allowed to impersonate the SA for local testing. Empty this after testing."
  type        = list(string)
  default     = []
}
