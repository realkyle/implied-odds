# Credentials come from the owner's IAM Identity Center (SSO) profile, never from keys in
# files: `aws sso login --profile ppm` before plan/apply.
provider "aws" {
  region  = var.region
  profile = var.aws_profile

  # Added to every resource that supports tags, so the bill and the console can be
  # filtered to this project.
  default_tags {
    tags = {
      Project   = "ppm"
      ManagedBy = "terraform"
    }
  }
}
