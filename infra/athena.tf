resource "aws_athena_workgroup" "reporting" {
  name          = "${var.name_prefix}-reporting"
  description   = "PaySim sample reporting; 100 MiB scan limit per query"
  state         = "ENABLED"
  force_destroy = false
  tags          = merge(local.tags, { Task = "T4" })

  configuration {
    bytes_scanned_cutoff_per_query     = 104857600
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    requester_pays_enabled             = false

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
    result_configuration {
      output_location       = "s3://${aws_s3_bucket.data["curated"].id}/athena-results/"
      expected_bucket_owner = var.aws_account_id
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
  depends_on = [aws_s3_bucket_public_access_block.data, aws_s3_bucket_server_side_encryption_configuration.data]
}

output "athena_reporting_workgroup" {
  value = aws_athena_workgroup.reporting.name
}
