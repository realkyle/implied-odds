variable "region" {
  description = "AWS region for the VM and the backup bucket."
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS CLI profile (IAM Identity Center / SSO) Terraform authenticates with."
  type        = string
  default     = "ppm"
}

variable "admin_cidr" {
  description = "The owner's public IP as a /32, the only address allowed to SSH in. Set it in terraform.tfvars (git-ignored); re-apply when the IP changes."
  type        = string

  validation {
    condition     = can(cidrhost(var.admin_cidr, 0)) && endswith(var.admin_cidr, "/32")
    error_message = "admin_cidr must be a single IPv4 address in CIDR form, e.g. 203.0.113.7/32."
  }
}

variable "ssh_public_key_path" {
  description = "Public half of the SSH key for the VM. Lightsail documents only ssh-rsa for imported keys, so use RSA 4096."
  type        = string
  default     = "~/.ssh/ppm_lightsail.pub"
}

variable "bundle_id" {
  description = "Lightsail plan. small_3_0 = 2 GB RAM, 2 vCPU, 60 GB SSD, 3 TB transfer, 1 IPv4, $12/month (checked 2026-11-13)."
  type        = string
  default     = "small_3_0"
}
