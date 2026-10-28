"""Glue 5.0 batch entrypoint. Service imports/calls exist only behind main()."""
from __future__ import annotations

import hashlib
import json
import sys
from urllib.parse import urlsplit

from pipeline.local import snapshot_bytes
from pipeline.transform import TABLE_COLUMNS, MONEY_FIELDS, transform_events, write_parquet

MAX_RAW_BYTES = 128 * 1024 * 1024


def s3_location(uri):
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/") or parsed.query or parsed.fragment:
        raise ValueError("expected an S3 bucket and object/prefix")
    return parsed.netloc, parsed.path.lstrip("/")


def read_manifest_events(s3, manifest_uri, raw_bucket):
    bucket, key = s3_location(manifest_uri)
    if bucket != raw_bucket or not key.startswith("manifests/"):
        raise ValueError("manifest must be in the configured raw bucket manifests/ prefix")
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        data = body.read(1_048_577)
        if len(data) > 1_048_576:
            raise ValueError("manifest exceeds 1 MiB")
        manifest = json.loads(data)
    finally:
        body.close()
    objects = manifest.get("events")
    if not isinstance(objects, list) or not 1 <= len(objects) <= 1000 or not all(isinstance(uri, str) for uri in objects):
        raise ValueError("manifest events must list 1–1000 raw JSONL object URIs")
    total_bytes = 0
    for uri in objects:
        bucket, key = s3_location(uri)
        if bucket != raw_bucket or not key.startswith("events/"):
            raise ValueError("events must be in the configured raw bucket events/ prefix")
        response = s3.get_object(Bucket=bucket, Key=key)
        total_bytes += int(response["ContentLength"])
        if total_bytes > MAX_RAW_BYTES:
            response["Body"].close()
            raise ValueError("raw snapshot exceeds 128 MiB bounded batch limit")
        body = response["Body"]
        try:
            for line in body.iter_lines():
                if line.strip():
                    yield json.loads(line)
        finally:
            body.close()


def catalog_columns(table):
    integers = {"step", "source_step", "row_number", "date_key", "type_key", "hour"}
    return [{"Name": name, "Type": "date" if name == "date" else "decimal(38,10)" if name in MONEY_FIELDS else
             "bigint" if name in integers else "string"} for name in TABLE_COLUMNS[table]]


def register_tables(glue, database, output):
    """Publish only after all four snapshots are written. No catalog append."""
    for name in TABLE_COLUMNS:
        table = {
            "Name": name, "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
            "PartitionKeys": [],
            "StorageDescriptor": {
                "Columns": catalog_columns(name), "Location": f"{output}/{name}/",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            },
        }
        try:
            glue.create_table(DatabaseName=database, TableInput=table)
        except glue.exceptions.AlreadyExistsException:
            glue.update_table(DatabaseName=database, TableInput=table, SkipArchive=True)


def run_batch(s3, glue, spark, *, manifest_uri, raw_bucket, output_root, database):
    tables = transform_events(read_manifest_events(s3, manifest_uri, raw_bucket))
    digest = hashlib.sha256(snapshot_bytes(tables)).hexdigest()
    output = f"{output_root.rstrip('/')}/snapshots/{digest}"
    write_parquet(spark, tables, output)
    register_tables(glue, database, output)
    return {"snapshot_id": digest, "fact_count": len(tables["fact_transactions"])}


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET", "RAW_MANIFEST", "OUTPUT_ROOT", "DATABASE"])
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    result = run_batch(boto3.client("s3"), boto3.client("glue"), context.spark_session,
                       manifest_uri=args["RAW_MANIFEST"], raw_bucket=args["RAW_BUCKET"],
                       output_root=args["OUTPUT_ROOT"], database=args["DATABASE"])
    job.commit()
    print(json.dumps(result, sort_keys=True))  # Counts/IDs only; no events or credentials.


if __name__ == "__main__":
    main()
