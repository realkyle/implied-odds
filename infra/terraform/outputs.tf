output "vm_ip" {
  description = "Static public IP of the recording VM."
  value       = aws_lightsail_static_ip.vm.ip_address
}

output "ssh_command" {
  description = "How to log in to the VM."
  value       = "ssh -i ${trimsuffix(pathexpand(var.ssh_public_key_path), ".pub")} ubuntu@${aws_lightsail_static_ip.vm.ip_address}"
}

output "backup_bucket" {
  description = "S3 bucket holding the off-machine backup."
  value       = aws_s3_bucket.backup.bucket
}

output "backup_user" {
  description = "IAM user for the VM's backup; create its access key by hand (see iam.tf)."
  value       = aws_iam_user.backup.name
}
