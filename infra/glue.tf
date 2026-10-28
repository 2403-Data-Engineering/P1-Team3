# Definition only: artifacts must be uploaded separately with explicit approval.
# No aws_s3_object resources: validate never packages or uploads local data.
resource "aws_glue_job" "batch" {
  name              = "${var.name_prefix}-batch"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10
  max_retries       = 0
  execution_class   = "STANDARD"
  tags              = merge(local.tags, { Task = "T2" })

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data["raw"].id}/artifacts/glue_job.py"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.data["raw"].id}/artifacts/paysim_pipeline.zip"
    "--additional-python-modules"        = "jsonschema==4.25.1"
    "--RAW_BUCKET"                       = aws_s3_bucket.data["raw"].id
    "--RAW_MANIFEST"                     = "s3://${aws_s3_bucket.data["raw"].id}/manifests/sample.json"
    "--OUTPUT_ROOT"                      = "s3://${aws_s3_bucket.data["curated"].id}/batch"
    "--DATABASE"                         = aws_glue_catalog_database.paysim.name
    "--TempDir"                          = "s3://${aws_s3_bucket.data["curated"].id}/tmp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue.name
    "--custom-logGroup-prefix"           = aws_cloudwatch_log_group.glue.name
    "--enable-auto-scaling"              = "false"
    "--enable-job-insights"              = "false"
  }

  depends_on = [
    aws_iam_role_policy.glue,
    aws_iam_role_policy.glue_runtime,
    aws_s3_bucket_public_access_block.data,
    aws_s3_bucket_server_side_encryption_configuration.data,
    aws_s3_bucket_policy.tls,
  ]
}

resource "aws_cloudwatch_log_group" "glue_runtime" {
  for_each          = toset(["error", "output"])
  name              = "${aws_cloudwatch_log_group.glue.name}/${each.key}"
  retention_in_days = 7
  tags              = merge(local.tags, { Task = "T2" })
}

# Glue 5 logging uses <custom prefix>/error and /output. Terraform precreates
# these groups with retention; the job may write streams only in these groups.
resource "aws_iam_role_policy" "glue_runtime" {
  name = "${var.name_prefix}-glue-runtime"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "GlueRuntimeLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = [for group in aws_cloudwatch_log_group.glue_runtime : "${group.arn}:*"]
      }
    ]
  })
}

resource "aws_glue_catalog_table" "raw_events" {
  name          = "raw_events"
  database_name = aws_glue_catalog_database.paysim.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification = "json"
    EXTERNAL       = "TRUE"
  }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data["raw"].id}/events/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
    dynamic "columns" {
      for_each = {
        schema_version = "string"
        event_id       = "string"
        txn_id         = "string"
        source_file    = "string"
        row_number     = "bigint"
        ingested_at    = "string"
        source_step    = "bigint"
        payload        = "struct<step:bigint,type:string,amount:decimal(38,10),nameOrig:string,oldbalanceOrg:decimal(38,10),newbalanceOrig:decimal(38,10),nameDest:string,oldbalanceDest:decimal(38,10),newbalanceDest:decimal(38,10)>"
      }
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}

output "glue_batch_job_name" {
  value = aws_glue_job.batch.name
}
