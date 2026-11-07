import hashlib
import os
import random
import time
import uuid
from typing import Dict, List
import boto3

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
HOT_BUCKET = os.getenv("S3_HOT_BUCKET", "hot-snapshots-tier")
COLD_BUCKET = os.getenv("S3_COLD_BUCKET", "cold-archive-tier")
MULTI_CLOUD_REPLICA_BUCKET = os.getenv("S3_REPLICA_BUCKET", "gcp-cloud-storage-replica")
TABLE_NAME = os.getenv("DYNAMODB_TABLE", "BackupMetadata")

s3_client = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT_URL,
    region_name=AWS_REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

dynamodb_client = boto3.client(
    "dynamodb",
    endpoint_url=AWS_ENDPOINT_URL,
    region_name=AWS_REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)


class SnapshotOrchestrator:
    @classmethod
    def _record_state(
        cls,
        snapshot_id: str,
        volume_id: str,
        state: str,
        size_mb: int,
        checksum: str,
        latency_ms: float,
        tier: str = "HOT",
    ):
        pk = f"tenant-corp#{volume_id}"
        sk = str(time.time())

        dynamodb_client.put_item(
            TableName=TABLE_NAME,
            Item={
                "PK": {"S": pk},
                "Timestamp": {"N": sk},
                "SnapshotID": {"S": snapshot_id},
                "State": {"S": state},
                "SizeMB": {"N": str(size_mb)},
                "Checksum": {"S": checksum},
                "LatencyMs": {"N": str(round(latency_ms, 2))},
                "Tier": {"S": tier},
            },
        )

    @classmethod
    def execute_snapshot_lifecycle(cls, volume_id: str) -> Dict:
        snapshot_id = f"snap-{uuid.uuid4().hex[:8]}"
        size_mb = random.randint(128, 1024)
        start_time = time.time()

        # Stage 1: TRIGGERED
        cls._record_state(snapshot_id, volume_id, "TRIGGERED", size_mb, "N/A", 0.0)

        # Stage 2: INGESTING
        time.sleep(0.05)
        cls._record_state(
            snapshot_id,
            volume_id,
            "INGESTING",
            size_mb,
            "N/A",
            (time.time() - start_time) * 1000,
        )

        # Stage 3: VERIFYING_CHECKSUM
        raw_payload = f"payload-{volume_id}-{snapshot_id}-{time.time()}".encode("utf-8")
        raw_payload += os.urandom(1024)
        checksum = hashlib.sha256(raw_payload).hexdigest()

        time.sleep(0.06)
        cls._record_state(
            snapshot_id,
            volume_id,
            "VERIFYING_CHECKSUM",
            size_mb,
            checksum,
            (time.time() - start_time) * 1000,
        )

        # Stage 4: S3_TIERING & Multi-Cloud Replication
        s3_key = f"tenant-corp/{volume_id}/{snapshot_id}.bin"
        is_cold = random.random() < 0.35  # ~35% archived to Glacier

        if is_cold:
            tier_label = "COLD"
            # Archive directly to cold bucket simulating lifecycle Glacier transition
            s3_client.put_object(
                Bucket=COLD_BUCKET,
                Key=s3_key,
                Body=raw_payload,
                StorageClass="GLACIER",
            )
            cls._record_state(
                snapshot_id,
                volume_id,
                "S3_TIERING",
                size_mb,
                checksum,
                (time.time() - start_time) * 1000,
                tier="COLD",
            )
        else:
            tier_label = "HOT"
            s3_client.put_object(
                Bucket=HOT_BUCKET,
                Key=s3_key,
                Body=raw_payload,
                StorageClass="STANDARD",
            )

        # Replicate to secondary multi-cloud bucket (GCP Cloud Storage replica)
        s3_client.put_object(
            Bucket=MULTI_CLOUD_REPLICA_BUCKET,
            Key=s3_key,
            Body=raw_payload,
            StorageClass="STANDARD",
        )

        total_latency = (time.time() - start_time) * 1000
        tier_display = (
            "COLD (AWS Glacier + GCP)" if is_cold else "HOT (AWS Standard + GCP)"
        )

        # Stage 5: COMPLETED
        cls._record_state(
            snapshot_id,
            volume_id,
            "COMPLETED",
            size_mb,
            checksum,
            total_latency,
            tier=tier_display,
        )

        return {
            "snapshot_id": snapshot_id,
            "volume_id": volume_id,
            "status": "COMPLETED",
            "tier": tier_display,
            "checksum": checksum,
            "duration_ms": round(total_latency, 2),
        }

    @classmethod
    def run_failover_recovery(cls, volume_id: str) -> Dict:
        start = time.time()
        pk = f"tenant-corp#{volume_id}"

        response = dynamodb_client.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": {"S": pk}},
            ScanIndexForward=False,
            Limit=20,
        )

        items = response.get("Items", [])
        completed = [i for i in items if i.get("State", {}).get("S") == "COMPLETED"]

        if not completed:
            raise ValueError(f"No valid snapshot points found for {volume_id}")

        latest_point = completed[0]
        snap_id = latest_point["SnapshotID"]["S"]
        tier = latest_point.get("Tier", {}).get("S", "HOT")
        expected_checksum = latest_point["Checksum"]["S"]

        s3_key = f"tenant-corp/{volume_id}/{snap_id}.bin"

        # Direct Glacier reads raise InvalidObjectState on synchronous GetObject.
        # Fallback to secondary multi-cloud standard replica bucket.
        if "COLD" in tier:
            source_bucket = MULTI_CLOUD_REPLICA_BUCKET
            retrieval_source = "GCP Cloud Storage Replica (Glacier Failover)"
        else:
            source_bucket = HOT_BUCKET
            retrieval_source = "AWS S3 Standard (Hot Tier)"

        obj = s3_client.get_object(Bucket=source_bucket, Key=s3_key)
        recovered_bytes = obj["Body"].read()

        actual_checksum = hashlib.sha256(recovered_bytes).hexdigest()
        is_valid = actual_checksum == expected_checksum
        latency_ms = (time.time() - start) * 1000

        return {
            "volume_id": volume_id,
            "snapshot_id": snap_id,
            "recovery_status": "SUCCESS" if is_valid else "CORRUPTED_FAILOVER",
            "restored_from_tier": retrieval_source,
            "rto_latency_ms": round(latency_ms, 2),
            "checksum_verified": is_valid,
            "recovery_timestamp": time.time(),
        }