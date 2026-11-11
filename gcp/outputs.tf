output "service_account_email" {
  value = google_service_account.cartography.email
}

output "project_number" {
  value = google_project.cartography.number
}

# Not a secret: an ADC "external_account" config. The Lambda writes this to /tmp
# and points GOOGLE_APPLICATION_CREDENTIALS at it; google-auth then signs an STS
# GetCallerIdentity with the Lambda's AWS creds and exchanges it for a GCP token.
output "lambda_credential_config" {
  value = jsonencode({
    type                              = "external_account"
    audience                          = "//iam.googleapis.com/${google_iam_workload_identity_pool_provider.aws.name}"
    subject_token_type                = "urn:ietf:params:aws:token-type:aws4_request"
    token_url                         = "https://sts.googleapis.com/v1/token"
    service_account_impersonation_url = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/${google_service_account.cartography.email}:generateAccessToken"
    credential_source = {
      environment_id                 = "aws1"
      region_url                     = "http://169.254.169.254/latest/meta-data/placement/availability-zone"
      url                            = "http://169.254.169.254/latest/meta-data/iam/security-credentials"
      regional_cred_verification_url = "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15"
    }
  })
}
