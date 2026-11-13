# The recording VM: one Lightsail instance, a static IP, and a firewall that only lets
# the owner SSH in.

# Only the PUBLIC key is uploaded. The private key never leaves the laptop and never
# appears in Terraform state.
resource "aws_lightsail_key_pair" "admin" {
  name       = "ppm-admin"
  public_key = file(pathexpand(var.ssh_public_key_path))
}

resource "aws_lightsail_instance" "vm" {
  name              = "ppm-vm"
  availability_zone = "${var.region}a"
  blueprint_id      = "ubuntu_24_04"
  bundle_id         = var.bundle_id
  key_pair_name     = aws_lightsail_key_pair.admin.name
  ip_address_type   = "ipv4" # no IPv6, so there is exactly one address family to firewall

  # Runs once, as root, on the first boot only (Docker, swap, data directories).
  user_data = file("${path.module}/cloud-init.sh")

  lifecycle {
    # The VM's disk holds recorded data that isn't backed up yet (S3 sync runs nightly).
    # Changing user_data or the blueprint would make Terraform destroy and recreate the
    # VM. The script only runs on first boot anyway, so a later edit couldn't take
    # effect on this VM. Ignore those changes and refuse any plan that destroys it.
    # To retire the VM on purpose, remove prevent_destroy in a reviewed commit first.
    ignore_changes  = [user_data, blueprint_id]
    prevent_destroy = true
  }
}

# A static IP stays the same across stop/start, so the SSH config and known_hosts keep
# working. It is free while it is attached to an instance.
resource "aws_lightsail_static_ip" "vm" {
  name = "ppm-vm-ip"
}

resource "aws_lightsail_static_ip_attachment" "vm" {
  static_ip_name = aws_lightsail_static_ip.vm.name
  instance_name  = aws_lightsail_instance.vm.name
}

# This resource REPLACES the instance's whole firewall, so the defaults (22 and 80 open
# to the world) are gone and only this rule remains: SSH from the owner's /32, plus
# Lightsail's own browser-SSH service ("lightsail-connect"), a fallback for when the
# owner's IP changes. Nothing listens on any other port (Redis binds to 127.0.0.1).
resource "aws_lightsail_instance_public_ports" "vm" {
  instance_name = aws_lightsail_instance.vm.name

  port_info {
    protocol          = "tcp"
    from_port         = 22
    to_port           = 22
    cidrs             = [var.admin_cidr]
    cidr_list_aliases = ["lightsail-connect"]
  }
}
