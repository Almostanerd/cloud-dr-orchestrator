from enum import Enum
from typing import List, Optional
from pydantic import BaseModel


class SnapshotState(str, Enum):
    TRIGGERED = "TRIGGERED"
    INGESTING = "INGESTING"
    VERIFYING_CHECKSUM = "VERIFYING_CHECKSUM"
    S3_TIERING = "S3_TIERING"
    COMPLETED = "COMPLETED"
    FAILED_RETRY = "FAILED_RETRY"


class SnapshotLog(BaseModel):
    snapshot_id: str
    tenant_id: str
    volume_id: str
    state: SnapshotState
    size_mb: int
    checksum: str
    latency_ms: float
    timestamp: float
    tier: str = "HOT"


class TelemetryResponse(BaseModel):
    active_tenant: str
    calculated_rpo_seconds: float
    calculated_rto_seconds: float
    total_snapshots_indexed: int
    sla_status: str
    recent_events: List[SnapshotLog]


class RecoveryResponse(BaseModel):
    volume_id: str
    snapshot_id: str
    recovery_status: str
    restored_from_tier: str
    rto_latency_ms: float
    checksum_verified: bool
    recovery_timestamp: float