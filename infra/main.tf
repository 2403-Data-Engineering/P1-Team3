resource "aws_budgets_budget" "monthly" {
  account_id   = var.aws_account_id
  name         = "${var.name_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  tags         = local.tags

  # Account-wide: catches untagged charges as well as the demo's costs.
  dynamic "notification" {
    for_each = toset(["50", "100"])
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = tonumber(notification.value)
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.budget_email]
    }
  }
}

resource "aws_s3_bucket" "data" {
  for_each      = toset(["raw", "curated"])
  bucket        = "${var.name_prefix}-${var.aws_account_id}-${var.aws_region}-${each.key}"
  force_destroy = false
  tags          = { DataLayer = each.key }
  depends_on    = [aws_budgets_budget.monthly]
}

resource "aws_s3_bucket_public_access_block" "data" {
  for_each                = aws_s3_bucket.data
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_policy" "tls" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

resource "aws_glue_catalog_database" "paysim" {
  name         = local.catalog_name
  description  = "PaySim dev raw and curated tables; sample only."
  location_uri = "s3://${aws_s3_bucket.data["curated"].id}/"
  tags         = local.tags
  depends_on   = [aws_budgets_budget.monthly]
}

resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/${var.name_prefix}"
  retention_in_days = 7
  tags              = local.tags
  depends_on        = [aws_budgets_budget.monthly]
}

resource "aws_iam_role" "glue" {
  name = "${var.name_prefix}-glue"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = var.aws_account_id } }
    }]
  })
  tags       = local.tags
  depends_on = [aws_budgets_budget.monthly]
}

resource "aws_iam_role_policy" "glue" {
  name = "${var.name_prefix}-data-access"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListProjectBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [for bucket in aws_s3_bucket.data : bucket.arn]
      },
      {
        Sid      = "ReadRaw"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${aws_s3_bucket.data["raw"].arn}/*"]
      },
      {
        Sid      = "WriteCurated"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = ["${aws_s3_bucket.data["curated"].arn}/*"]
      },
      {
        Sid    = "ProjectCatalog"
        Effect = "Allow"
        Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition", "glue:CreatePartition", "glue:BatchCreatePartition", "glue:UpdatePartition"]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:catalog",
          aws_glue_catalog_database.paysim.arn,
          "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/${local.catalog_name}/*"
        ]
      },
      {
        Sid      = "ProjectLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["${aws_cloudwatch_log_group.glue.arn}:*"]
      }
    ]
  })
}
