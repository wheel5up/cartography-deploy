terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # State lives in the bucket created by main.tf (google_storage_bucket.tfstate,
  # "<project_id>-tfstate"). Bootstrap with local state (comment this block out),
  # then: terraform init -backend-config=backend.hcl -migrate-state
  backend "gcs" {}
}

provider "google" {
  # Uses your gcloud ADC: `gcloud auth application-default login`
}
