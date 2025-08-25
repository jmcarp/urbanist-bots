terraform {
  required_version = ">= 1.11"

  required_providers {
    oxide = {
      source  = "oxidecomputer/oxide"
      version = ">= 0.13.0"
    }
  }
}

provider "oxide" {
  profile = "r3"
}

data "oxide_project" "project" {
  name = "carp"
}

data "oxide_vpc" "default" {
  project_name = data.oxide_project.project.name
  name         = "default"
}

data "oxide_vpc_subnet" "default" {
  project_name = data.oxide_project.project.name

  vpc_name = data.oxide_vpc.default.name
  name     = "default"
}

data "oxide_image" "os" {
  name = "noble"
}

data "oxide_ssh_key" "key" {
  name = "carp"
}

resource "oxide_disk" "playground-boot" {
  project_id = data.oxide_project.project.id

  name        = "playground-boot"
  description = "playground-boot"

  size            = 20 * pow(2, 30)
  source_image_id = data.oxide_image.os.id
}

resource "oxide_disk" "playground-data" {
  project_id = data.oxide_project.project.id

  name        = "playground-data"
  description = "playground-data"

  size       = 200 * pow(2, 30)
  block_size = 4096
}

data "cloudinit_config" "user_data" {
  base64_encode = true

  part {
    filename     = "cloud-config.yaml"
    content_type = "text/cloud-config"

    content = <<EOT
#cloud-config
fs_setup:
- device: /dev/nvme1n1
  filesystem: ext4
  label: data
mounts:
- [/dev/nvme1n1, /data, auto, "defaults,nofail"]
mount_default_fields: [None, None, "auto", "defaults,nofail", "0", "2"]
EOT
  }
}

resource "oxide_instance" "playground" {
  project_id  = data.oxide_project.project.id
  description = "Playground instance."
  name        = "playground"
  host_name   = "playground"

  memory       = 8 * pow(2, 30)
  ncpus        = 4
  boot_disk_id = oxide_disk.playground-boot.id
  disk_attachments = [
    oxide_disk.playground-boot.id,
    oxide_disk.playground-data.id,
  ]

  ssh_public_keys = [data.oxide_ssh_key.key.id]

  network_interfaces = [
    {
      name        = "net0"
      description = "net0"
      vpc_id      = data.oxide_vpc.default.id
      subnet_id   = data.oxide_vpc_subnet.default.id
    },
  ]

  external_ips = [
    { type = "ephemeral" },
  ]

  user_data = data.cloudinit_config.user_data.rendered
}
