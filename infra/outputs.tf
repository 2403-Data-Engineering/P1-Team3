output "raw_bucket" {
  value = aws_s3_bucket.data["raw"].id
}

output "curated_bucket" {
  value = aws_s3_bucket.data["curated"].id
}

output "glue_database" {
  value = aws_glue_catalog_database.paysim.name
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "glue_log_group" {
  value = aws_cloudwatch_log_group.glue.name
}

output "budget_name" {
  value = aws_budgets_budget.monthly.name
}
