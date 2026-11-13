# Terraform and provider versions.
#
# `~> 6.70` allows any 6.x from 6.70 up (bug fixes, new resources) but never 7.0, which
# may break things. The exact version actually used is pinned in .terraform.lock.hcl
# (committed), so every `terraform init` gets the same provider build.
terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.70"
    }
  }
}
