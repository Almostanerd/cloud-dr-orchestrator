import hashlib
import logging
import random
import time
import uuid
from typing import Dict
from app.aws_client import (
    COLD_BUCKET,
    HOT_BUCKET,
    MULTI_CLOUD_REPLICA_BUCKET,
    TABLE_NAME,
    dynamodb_client,
    s3_client,
)
from app.models import SnapshotLog, SnapshotState

logger = logging.getLogger("orchestrator")
logger.setLevel(logging.INFO)

VOLUMES_CATALOG: Dict[str, Dict] = {
    f"vol-{i:03d}": {"tenant_id": "tenant-corp", "size_mb": 128 * i}
    for i in range(1, 6)
}


class OrchestratorEngine:

    @staticmethod
    def _persist_state_telemetry(log: SnapshotLog):
        pk = f"{log.tenant_id}#{log.volume_id}"
        dynamodb_client.put_item(
            TableName=TABLE_NAME,
            Item={
                "PK": {"S": pk},
                "Timestamp": {"N": f"{log.timestamp:.4f}"},
                "SnapshotID": {"S": log.snapshot_id},
                "State": {"S": log.state.value},
                "SizeMB": {"N": str(log.size_mb)},
                "Checksum": {"S": log.checksum},
                "LatencyMs": {"N": f"{log.latency_ms:.2f}"},
                "Tier": {"S": log.tier},
            },
        )

    @classmethod
    def run_snapshot_workflow(cls, volume_id: str):
        vol = VOLUMES_CATALOG[volume_id]
        tenant_id = vol["tenant_id"]
        size_mb = vol["size_mb"]
        snapshot_id = f"snap-{uuid.uuid4().hex[:8]}"
        start_time = time.time()

        def record(state: SnapshotState, checksum: str = "N/A", tier: str = "HOT"):
            current_time = time.time()
            elapsed_ms = (current_time - start_time) * 1000
            cls._persist_state_telemetry(
                SnapshotLog(
                    snapshot_id=snapshot_id,
                    tenant_id=tenant_id,
                    volume_id=volume_id,
                    state=state,
                    size_mb=size_mb,
                    checksum=checksum,
                    latency_ms=elapsed_ms,
                    timestamp=current_time,
                    tier=tier,
                )
            )

        try:
            # 1. TRIGGERED
            record(SnapshotState.TRIGGERED)
            time.sleep(0.04)

            # 2. INGESTING
            record(SnapshotState.INGESTING)
            synthetic_payload = f"payload-{tenant_id}-{volume_id}-{snapshot_id}".encode("utf-8")
            s3_key = f"{tenant_id}/{volume_id}/{snapshot_id}.bin"
            time.sleep(0.05)

            # 3. VERIFYING_CHECKSUM
            checksum = hashlib.sha256(synthetic_payload).hexdigest()
            record(SnapshotState.VERIFYING_CHECKSUM, checksum=checksum)

            # 4. S3_TIERING (Store in AWS Hot Tier)
            s3_client.put_object(
                Bucket=HOT_BUCKET,
                Key=s3_key,
                Body=synthetic_payload,
                Metadata={"checksum": checksum, "volume_id": volume_id},
            )

            # Cross-Cloud DR Backup (Simulating secondary cloud sync)
            s3_client.put_object(
                Bucket=MULTI_CLOUD_REPLICA_BUCKET,
                Key=s3_key,
                Body=synthetic_payload,
                Metadata={"checksum": checksum, "provider": "gcp-replica"},
            )

            # Lifecycle Policy: 40% transitioned to S3 Glacier
            assigned_tier = "HOT (AWS Standard + GCP)"
            if random.random() < 0.40:
                record(SnapshotState.S3_TIERING, checksum=checksum, tier="COLD")
                s3_client.copy_object(
                    Bucket=COLD_BUCKET,
                    CopySource={"Bucket": HOT_BUCKET, "Key": s3_key},
                    Key=s3_key,
                    StorageClass="GLACIER",
                )
                s3_client.delete_object(Bucket=HOT_BUCKET, Key=s3_key)
                assigned_tier = "COLD (AWS Glacier + GCP)"

            # 5. COMPLETED
            record(SnapshotState.COMPLETED, checksum=checksum, tier=assigned_tier)
            logger.info(f"Snapshot {snapshot_id} finalized and replicated across multi-cloud.")

        except Exception as exc:
            logger.error(f"Snapshot failure on {volume_id}: {str(exc)}")
            record(SnapshotState.FAILED_RETRY)

    @classmethod
    def execute_fleet_backup(cls):
        for volume_id in VOLUMES_CATALOG.keys():
            cls.run_snapshot_workflow(volume_id)

    @classmethod
    def run_failover_recovery(cls, volume_id: str) -> Dict:
        start = time.time()
        pk = f"tenant-corp#{volume_id}"

        response = dynamodb_client.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": {"S": pk}},
            ScanIndexForward=False,
            Limit=10,
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
        source_bucket = COLD_BUCKET if "COLD" in tier else HOT_BUCKET

        obj = s3_client.get_object(Bucket=source_bucket, Key=s3_key)
        recovered_bytes = obj["Body"].read()

        actual_checksum = hashlib.sha256(recovered_bytes).hexdigest()
        is_valid = (actual_checksum == expected_checksum)
        latency_ms = (time.time() - start) * 1000

        return {
            "volume_id": volume_id,
            "snapshot_id": snap_id,
            "recovery_status": "SUCCESS" if is_valid else "CORRUPTED_FAILOVER",
            "restored_from_tier": tier,
            "rto_latency_ms": round(latency_ms, 2),
            "checksum_verified": is_valid,
            "recovery_timestamp": time.time(),
        }