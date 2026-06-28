output "warehouse_bucket" {
  description = "S3 bucket backing the Iceberg warehouse."
  value       = aws_s3_bucket.warehouse.bucket
}

output "glue_database" {
  description = "Glue catalog database for Iceberg tables."
  value       = aws_glue_catalog_database.lakehouse.name
}

output "kafka_bootstrap_arn" {
  description = "MSK Serverless cluster ARN (use with IAM SASL bootstrap)."
  value       = aws_msk_serverless_cluster.kafka.arn
}

output "streaming_job_role_arn" {
  description = "IAM role ARN for the Spark streaming job."
  value       = aws_iam_role.streaming_job.arn
}
