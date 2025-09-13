terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
  required_version = ">= 1.0"
}



###################
# VARIABLES
###################

variable "access_key" {
  description = "AWS access key"
  type        = string
  sensitive   = true
}

variable "secret_key" {
  description = "AWS secret key"
  type        = string
  sensitive   = true
}

variable "session_token" {
  description = "AWS session token"
  type        = string
  sensitive   = true
}

variable "region" {
  default = "us-east-1"
}

variable "key_name" {
  default     = "hi"
  description = "EC2 key pair name"
}

variable "ubuntu_ami" {
  default = "ami-020cba7c55df1f615" # Ubuntu 22.04 LTS
}

variable "workers_count" {
  default = 5
}
###################
# PROVIDER
###################
provider "aws" {
  region = "us-east-1"

  access_key = var.access_key
  secret_key = var.secret_key
  token      = var.session_token
}

###################
# VPC + SUBNETS
###################
resource "aws_vpc" "gridmr_vpc" {
  cidr_block           = "172.16.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = { Name = "TF_gridmr_vpc-vpc" }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.gridmr_vpc.id
  tags   = { Name = "TF_gridmr_vpc-igw" }
}

resource "aws_subnet" "public_a" {
  vpc_id                  = aws_vpc.gridmr_vpc.id
  cidr_block              = "172.16.1.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true
  tags                    = { Name = "TF_gridmr_vpc-subnet-public1-us-east-1a" }
}

resource "aws_subnet" "private_a1" {
  vpc_id            = aws_vpc.gridmr_vpc.id
  cidr_block        = "172.16.2.0/24"
  availability_zone = "us-east-1a"
  tags              = { Name = "TF_gridmr_vpc-subnet-private1-us-east-1a" }
}

# Public route table
resource "aws_route_table" "public_rt" {
  vpc_id = aws_vpc.gridmr_vpc.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
  tags = { Name = "TF_public-rt" }
}

resource "aws_route_table_association" "public_a_assoc" {
  subnet_id      = aws_subnet.public_a.id
  route_table_id = aws_route_table.public_rt.id
}

###################
# NAT for private subnet egress
###################
resource "aws_eip" "nat_eip" {
  vpc = true
  tags = { Name = "TF_gridmr_nat_eip" }
}

resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat_eip.id
  subnet_id     = aws_subnet.public_a.id
  depends_on    = [aws_internet_gateway.igw]
  tags          = { Name = "TF_gridmr_nat" }
}

resource "aws_route_table" "private_rt" {
  vpc_id = aws_vpc.gridmr_vpc.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat.id
  }
  tags = { Name = "TF_private-rt" }
}

resource "aws_route_table_association" "private_a1_assoc" {
  subnet_id      = aws_subnet.private_a1.id
  route_table_id = aws_route_table.private_rt.id
}

###################
# SECURITY GROUPS
###################
resource "aws_security_group" "efs_sg" {
  name        = "efs-sg"
  description = "Allow NFS"
  vpc_id      = aws_vpc.gridmr_vpc.id

  ingress {
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    cidr_blocks     = ["172.16.0.0/16"] # internal
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "TF_EFS_SG" }
}

resource "aws_security_group" "master_sg" {
  name        = "master-sg"
  description = "Allow SSH and internal traffic"
  vpc_id      = aws_vpc.gridmr_vpc.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # HTTP job submission to the master (default port 8080)
  ingress {
    description = "HTTP job submit"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description  = "Internal"
    from_port    = 0
    to_port      = 0
    protocol     = "-1"
    cidr_blocks  = ["172.16.0.0/16"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "TF_MasterSG" }
}

resource "aws_security_group" "worker_sg" {
  name        = "worker-sg"
  description = "Allow internal"
  vpc_id      = aws_vpc.gridmr_vpc.id

  ingress {
    description  = "Internal"
    from_port    = 0
    to_port      = 0
    protocol     = "-1"
    cidr_blocks  = ["172.16.0.0/16"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "TF_WorkerSG" }
}

###################
# EFS
###################
resource "aws_efs_file_system" "gridmr" {
  creation_token = "gridmr-efs"
  throughput_mode = "bursting"
  tags = {
    Name = "TF_GridMR_EFS"
  }
}

resource "aws_efs_mount_target" "gridmr_mt" {
  file_system_id  = aws_efs_file_system.gridmr.id
  subnet_id       = aws_subnet.private_a1.id # o public_a, elige una
  security_groups = [aws_security_group.efs_sg.id]

  lifecycle {
    create_before_destroy = true
  }
}

###################
# EC2 Master
###################
data "aws_availability_zones" "available" {}

resource "aws_instance" "master" {
  ami                         = var.ubuntu_ami
  instance_type               = "t2.micro"
  subnet_id                   = aws_subnet.public_a.id
  private_ip                  = "172.16.1.10"
  associate_public_ip_address = true
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.master_sg.id, aws_security_group.efs_sg.id]

  user_data = <<-EOF
              #!/bin/bash
              set -eux
              apt-get update -y
              apt-get install -y nfs-common
              mkdir -p /shared
              echo "${aws_efs_file_system.gridmr.dns_name}:/ /shared nfs4 defaults,_netdev,nofail,x-systemd.automount 0 0" >> /etc/fstab
              systemctl daemon-reload
              sleep 100
              mount -a
              systemctl daemon-reload
              systemctl restart remote-fs.target || true
              sudo chown ubuntu:ubuntu /shared
              sudo chmod 777 /shared
              git clone https://github.com/SebasUr/GridMR.git && cd GridMR && git switch dev && git switch feature/fsmigrate && cd deployment/scripts && sudo ./master.sh
              EOF

  tags = {
    Name = "TF_Master"
  }
}

###################
# EC2 Workers
###################
resource "aws_instance" "workers" {
  count                       = var.workers_count
  ami                         = var.ubuntu_ami
  instance_type               = "t2.micro"
  subnet_id                   = aws_subnet.private_a1.id
  associate_public_ip_address = false
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.worker_sg.id, aws_security_group.efs_sg.id]

  user_data = <<-EOF
              #!/bin/bash
              sleep 100
              set -eux
              apt-get update -y
              apt-get install -y nfs-common
              mkdir -p /shared
              echo "${aws_efs_file_system.gridmr.dns_name}:/ /shared nfs4 defaults,_netdev,nofail,x-systemd.automount 0 0" >> /etc/fstab
              mount -a
              systemctl daemon-reload
              systemctl restart remote-fs.target || true
              sudo chown ubuntu:ubuntu /shared
              sudo chmod 777 /shared
              export HOSTNAME
              git clone https://github.com/SebasUr/GridMR.git && cd GridMR && git switch dev && git switch feature/fsmigrate && cd deployment/scripts && sudo ./worker.sh
              EOF

  tags = {
    Name = "TF_Worker-${count.index + 1}"
  }
}

###################
# OUTPUTS
###################
output "master_public_ip" {
  value = aws_instance.master.public_ip
}

output "master_private_ip" {
  value = aws_instance.master.private_ip
}

output "workers_private_ips" {
  value = [for w in aws_instance.workers : w.private_ip]
}

output "efs_dns_name" {
  value = aws_efs_file_system.gridmr.dns_name
}
