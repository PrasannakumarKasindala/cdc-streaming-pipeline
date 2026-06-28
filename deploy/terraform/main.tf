# Example production footprint for cdc-streaming-pipeline on AWS.
# Provisions the Iceberg-on-S3 warehouse, a Glue catalog database, an MSK
# Serverless (Kafka) cluster, and an IAM role the Spark streaming job assumes.
#
# EXAMPLE ONLY -- this has not been applied. Review, set a backend, and run
# `terraform plan` against your own account before using.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # backend "s3" { bucket = "..." key = "cdc/terraform.tfstate" region = "us-east-1" }
}

provider "aws" {
  region = var.region
}

locals {
  name = "${var.project}-${var.environment}"
  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# --- Iceberg warehouse (S3) ------------------------------------------------
resource "aws_s3_bucket" "warehouse" {
  bucket = "${local.name}-lakehouse"
  tags   = local.tags
}

resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  bucket                  = aws_s3_bucket.warehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Glue catalog database (Iceberg tables register here) ------------------
resource "aws_glue_catalog_database" "lakehouse" {
  name        = replace("${local.name}_lakehouse", "-", "_")
  description = "Iceberg catalog for ${local.name}"
}

# --- Kafka (MSK Serverless, IAM auth) --------------------------------------
resource "aws_msk_serverless_cluster" "kafka" {
  cluster_name = "${local.name}-kafka"

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  client_authentication {
    sasl { iam { enabled = true } }
  }

  tags = local.tags
}

# --- IAM role for the Spark streaming job ----------------------------------
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"] # or EMR/EKS execution principal
    }
  }
}

resource "aws_iam_role" "streaming_job" {
  name               = "${local.name}-streaming-job"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "streaming_job" {
  statement {
    sid     = "WarehouseRW"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.warehouse.arn,
      "${aws_s3_bucket.warehouse.arn}/*",
    ]
  }
  statement {
    sid     = "GlueCatalog"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:CreateTable",
               "glue:UpdateTable", "glue:GetTables", "glue:GetPartitions"]
    resources = ["*"]
  }
  statement {
    sid       = "KafkaConnect"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeTopic",
                 "kafka-cluster:ReadData", "kafka-cluster:DescribeGroup",
                 "kafka-cluster:AlterGroup"]
    resources = ["${aws_msk_serverless_cluster.kafka.arn}/*"]
  }
}

resource "aws_iam_role_policy" "streaming_job" {
  name   = "${local.name}-streaming-job"
  role   = aws_iam_role.streaming_job.id
  policy = data.aws_iam_policy_document.streaming_job.json
}
