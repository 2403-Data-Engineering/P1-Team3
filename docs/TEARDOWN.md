# Teardown runbook

**Current delivery: not applicable.** The user confirmed nothing was deployed.
No cloud discovery, stop, deletion or destroy was performed to complete T5.
Local tests/fakes and Terraform definitions are not paid resources. This is not
an assertion about unrelated resources in an AWS account.

The following is a future operator procedure, **not permission to execute it**.
Get explicit session approval before destruction or data deletion. Never run a
destroy command merely because this file or a future task mentions teardown.

1. Confirm the exact dev account, region, real Terraform checkout/backend/state
   and expected resource inventory. Do not use `infra/.terraform/plan-only`, an
   empty scratch state or a different workspace to destroy a deployment. Review
   actual state/resource ownership; the current 21-resource design count is only
   an expectation. Keep the separate state bucket available and protected.
2. Identify and stop future job triggers, active Glue runs, Athena queries and
   BI refreshes through separately authorized actions. Also inspect separately
   created streaming/graph/Redshift resources if any were later approved; none
   is part of the delivered runtime. Preserve needed data, run manifests,
   results and billing records before requesting deletion approval.
3. Obtain explicit approval to empty the two project data buckets, including
   raw events, evaluation data if colocated, curated/historical graph snapshots,
   job artifacts/temp objects and `athena-results/`. Bucket `force_destroy=false`
   deliberately blocks deletion while objects remain. If versioning was enabled
   later, review versions/delete markers as well. Abort approved incomplete
   multipart uploads. Do not delete an external state bucket or any computed
   bucket/path without verifying its resolved identity and scope first.
4. Review catalog objects created outside Terraform: the four T2 curated tables,
   reporting source tables/partitions and views created from SQL. Arrange their
   approved removal/dependency handling before deleting the database. These
   objects are not individually tracked in this stack. A future Athena workgroup
   may require query/named-query cleanup because `force_destroy=false`. Include
   both Glue runtime log groups and the root log group in retention decisions.
5. Prepare `terraform plan -destroy` in the **real initialized deployment** and
   review every deletion. Run `terraform destroy` only after explicit approval
   for that session and plan. Keep the budget until billable dependents are
   removed; dependencies help ordering but do not prove teardown success. Never
   paste credentials or unredacted state/plan contents into logs or documentation.
6. Confirm approved buckets, jobs, workgroup, catalog database, role/policies,
   log groups and budget are removed. Separately created resources require their
   own approval/verification. Retain the final state, provider lock and external
   versioned state bucket for recovery unless separate approval covers removal.
   Check billing after its reporting delay and record any remaining charges.
7. Update AGENT_HANDOFF.md with the actual commands/results, retained objects,
   outstanding costs and evidence. Mark live teardown complete only on evidence;
   do not convert “nothing deployed in repo-only sessions” into a live audit.

For local artifacts, keep `.env` private, use fresh output paths, and do not
remove prior export directories automatically. Incomplete T0.5 exports and T3
run directories are retained for inspection; deletion requires a separate,
specific scope. Deleting local Terraform state is not cloud teardown.

The shorter infrastructure instructions remain in [infra/README.md](../infra/README.md).
