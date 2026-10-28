# T1 foundation (design and plan only)

No resources have been provisioned by writing these files. Terraform >= 1.10
is required. The AWS provider lock file records the tested provider version.
AWS credentials belong in the standard AWS credential chain, never Terraform
source, tfvars, backend files, command arguments, or logs. Terraform does not
automatically load the root `.env.example`; it documents variable names only.

## Validate

From the repository root:

```powershell
terraform -chdir=infra init -backend=false -input=false
terraform -chdir=infra fmt -check
terraform -chdir=infra validate
```

Initialization downloads the provider but creates no AWS resources. Validation
does not need AWS credentials. Do not invent credentials or disable provider
credential checks to claim a successful AWS plan.

## Plan without remote state

Supply `TF_VAR_aws_account_id` for the explicitly selected dev account and
`TF_VAR_budget_email` in the process environment. Region defaults to us-east-1.
The provider rejects credentials from another account. Do not save a plan file:
plans/state can contain the budget recipient and other configuration values.

The root configuration declares an S3 backend, but its bucket is deliberately
unspecified. For the initial review, copy only `*.tf` and `.terraform.lock.hcl`
into an ignored `infra/.terraform/plan-only/` directory. In that copy only,
replace the `backend "s3"` block with `backend "local" {}`. Then run:

```powershell
terraform '-chdir=infra/.terraform/plan-only' init -input=false
terraform '-chdir=infra/.terraform/plan-only' plan -input=false -no-color
```

This is a creation preview against empty local state. It is not a drift check
for an existing deployment. Do not apply from this disposable directory. A real
deployment needs a separately managed state bucket and the backend setup below.
Do not run plan when credentials are absent; report validation results instead.

T1's original creation inventory was **15 resources**: one budget; two S3 buckets and
two each of public-access blocks, ownership controls, encryption configurations,
and TLS-only policies; one Glue database; one log group; one IAM role and one
inline role policy. T2 now adds a Glue job definition, one runtime log policy,
two log groups, and a raw catalog table: **20 resources** expected in the combined
design. T4 adds one Athena workgroup (100 MiB per-query scan cutoff, enforced
encrypted result settings), bringing the combined design to **21 resources**,
before any future deployment. No crawler, Kinesis,
Redshift, VPC, or Neo4j instance. Bucket names include account and region.

The Glue role reads raw objects, reads/writes/deletes curated objects, updates
only the project catalog, and writes to the project log groups. T2 configures
its job to use these groups and buckets for scripts/temp data. See `glue.tf`
and `pipeline/README.md` for the artifact and manifest protocol. No broad managed Glue policy is
attached. The future ingestion operator needs its own scoped upload permissions.

## State backend and later deployment

`backend.hcl.example` documents an existing state bucket, encryption, and
`use_lockfile = true`. Native S3 locking requires Terraform 1.10+; the backend
identity needs GetObject/PutObject on the state key and GetObject/PutObject/
DeleteObject on its `.tflock` key. See the [HashiCorp S3 backend documentation](https://developer.hashicorp.com/terraform/language/backend/s3).
Use a private, encrypted, versioned state bucket outside this stack so teardown
does not remove state. This session does not bootstrap or inspect that bucket.

Only after a separate approval and state-bucket availability, configure a local
ignored `backend.hcl` and initialize the real backend. Backend initialization
can write state/locks; do not do it as part of a read-only T1 review. No secrets
belong in backend configuration. Keep `.terraform.lock.hcl` under version control;
ignore `.terraform`, state, plans, and local input files.

The $5 account-wide monthly budget sends actual-spend alerts above 50% and 100%.
It includes unrelated account spending, catches untagged usage, and is not a hard
spending cap. Alerts can lag billing. Future resource creation depends on budget
creation, but a later approved deployment must confirm the alert destination
and budget exist before any billable jobs. T2 defines a job but no job or other
resource has been provisioned or executed in these sessions.

## Teardown runbook (documentation only; separate approval required)

Nothing was deployed for the repo-only delivery, so no teardown was performed.
The full inventory/data/catalog/workgroup/state procedure is in
[docs/TEARDOWN.md](../docs/TEARDOWN.md). The steps below are future instructions.

1. Confirm the dev account, region, backend/state, and exact resource inventory.
   Stop any later jobs/producers and preserve any data the user needs.
2. Obtain explicit approval for deletion, including data loss. Both data buckets
   have `force_destroy = false`; nonempty buckets deliberately block removal.
   Empty only the approved project buckets, including versions/delete markers
   if versioning was subsequently enabled, and abort incomplete multipart uploads.
3. In the real initialized stack, prepare and review `terraform plan -destroy`.
   Execute `terraform destroy` only with explicit authorization for that session.
   These are future operator instructions, not permission to run them now.
4. Confirm the data buckets, catalog/tables, log group, role/policy, and budget
   are removed. The dependency graph keeps the budget until dependent resources
   are removed. Check for separately created jobs, query outputs, or resources
   outside this stack; their deletion needs their own review.
5. Retain the external state bucket and final state for recovery unless separate
   approval covers their removal. Confirm billing after its reporting delay.
