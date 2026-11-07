import os
import time
from contextlib import asynccontextmanager
from typing import Dict, List
from apscheduler.schedulers.background import BackgroundScheduler
import boto3
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from app.orchestrator import (
    COLD_BUCKET,
    HOT_BUCKET,
    MULTI_CLOUD_REPLICA_BUCKET,
    TABLE_NAME,
    SnapshotOrchestrator,
    dynamodb_client,
    s3_client,
)

VOLUMES_CATALOG = ["vol-001", "vol-002", "vol-003", "vol-004", "vol-005"]


def init_aws_infrastructure():
    # Create Buckets
    existing_buckets = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]
    for b in [HOT_BUCKET, COLD_BUCKET, MULTI_CLOUD_REPLICA_BUCKET]:
        if b not in existing_buckets:
            s3_client.create_bucket(Bucket=b)

    # Create DynamoDB Telemetry Table
    existing_tables = dynamodb_client.list_tables().get("TableNames", [])
    if TABLE_NAME not in existing_tables:
        dynamodb_client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "Timestamp", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "Timestamp", "AttributeType": "N"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )


def automated_dr_orchestration_cycle():
    for vol in VOLUMES_CATALOG:
        try:
            SnapshotOrchestrator.execute_snapshot_lifecycle(vol)
        except Exception as e:
            print(f"Cycle execution error on {vol}: {e}")


scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_aws_infrastructure()
    automated_dr_orchestration_cycle()
    scheduler.add_job(
        automated_dr_orchestration_cycle, "interval", seconds=25, id="dr_worker"
    )
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(
    title="Enterprise Cloud Storage & Disaster Recovery Orchestrator",
    lifespan=lifespan,
)


class SnapshotLog(BaseModel):
    snapshot_id: str
    tenant_id: str
    volume_id: str
    state: str
    size_mb: int
    checksum: str
    latency_ms: float
    timestamp: float
    tier: str


class TelemetryResponse(BaseModel):
    active_tenant: str
    calculated_rpo_seconds: float
    calculated_rto_seconds: float
    total_snapshots_indexed: int
    sla_status: str
    recent_events: List[SnapshotLog]


@app.get("/health")
def health_check():
    return {
        "status": "HEALTHY",
        "service": "DR-Orchestrator",
        "active_volumes": VOLUMES_CATALOG,
        "engine": "FastAPI + LocalStack (S3 & DynamoDB)",
    }


@app.get("/telemetry", response_model=TelemetryResponse)
def get_telemetry():
    response = dynamodb_client.scan(TableName=TABLE_NAME)
    items = response.get("Items", [])

    events = [
        SnapshotLog(
            snapshot_id=item["SnapshotID"]["S"],
            tenant_id=item["PK"]["S"].split("#")[0],
            volume_id=item["PK"]["S"].split("#")[1],
            state=item["State"]["S"],
            size_mb=int(item["SizeMB"]["N"]),
            checksum=item["Checksum"]["S"],
            latency_ms=float(item["LatencyMs"]["N"]),
            timestamp=float(item["Timestamp"]["N"]),
            tier=item.get("Tier", {}).get("S", "HOT"),
        )
        for item in items
    ]

    # Sort descending by timestamp so latest transitions appear first
    events.sort(key=lambda x: x.timestamp, reverse=True)

    completed_timestamps = sorted(
        [e.timestamp for e in events if e.state == "COMPLETED"]
    )
    calculated_rpo = 25.0
    if len(completed_timestamps) > 1:
        deltas = [
            t2 - t1
            for t1, t2 in zip(completed_timestamps[:-1], completed_timestamps[1:])
        ]
        calculated_rpo = sum(deltas) / len(deltas)

    completed_latencies = [e.latency_ms for e in events if e.state == "COMPLETED"]
    calculated_rto = (
        (sum(completed_latencies) / len(completed_latencies) / 1000)
        if completed_latencies
        else 0.15
    )

    sla_status = "MEETING_SLA" if calculated_rpo <= 60.0 else "SLA_BREACHED"

    return TelemetryResponse(
        active_tenant="tenant-corp",
        calculated_rpo_seconds=round(calculated_rpo, 2),
        calculated_rto_seconds=round(calculated_rto, 2),
        total_snapshots_indexed=len(events),
        sla_status=sla_status,
        recent_events=events[:25],
    )


@app.post("/recover/{volume_id}")
def trigger_recovery(volume_id: str):
    if volume_id not in VOLUMES_CATALOG:
        raise HTTPException(status_code=404, detail="Volume ID not registered.")
    try:
        recovery_report = SnapshotOrchestrator.run_failover_recovery(volume_id)
        return recovery_report
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"DR Failover error: {str(e)}"
        )


@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Enterprise Cloud DR Orchestrator</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <meta http-equiv="refresh" content="8">
    </head>
    <body class="bg-slate-900 text-slate-100 font-sans p-6">
        <div class="max-w-7xl mx-auto">
            <header class="mb-8 flex justify-between items-center border-b border-slate-800 pb-4">
                <div>
                    <h1 class="text-3xl font-extrabold text-indigo-400">Enterprise Cloud DR Orchestrator</h1>
                    <p class="text-sm text-slate-400">AWS Multi-Tier Replication & Disaster Recovery Monitor</p>
                </div>
                <div id="sla-badge" class="px-4 py-2 rounded-full font-bold text-xs uppercase bg-emerald-950 text-emerald-400 border border-emerald-800">
                    SLA Nominal
                </div>
            </header>

            <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
                <div class="bg-slate-800/80 p-5 rounded-xl border border-slate-700">
                    <span class="text-xs font-semibold text-slate-400 uppercase">Calculated RPO</span>
                    <p id="rpo-val" class="text-3xl font-bold text-white mt-2">-- s</p>
                    <span class="text-xs text-emerald-400">Target: &lt; 60s</span>
                </div>
                <div class="bg-slate-800/80 p-5 rounded-xl border border-slate-700">
                    <span class="text-xs font-semibold text-slate-400 uppercase">Calculated RTO</span>
                    <p id="rto-val" class="text-3xl font-bold text-white mt-2">-- s</p>
                    <span class="text-xs text-emerald-400">Target: &lt; 2s</span>
                </div>
                <div class="bg-slate-800/80 p-5 rounded-xl border border-slate-700">
                    <span class="text-xs font-semibold text-slate-400 uppercase">Indexed State Transitions</span>
                    <p id="count-val" class="text-3xl font-bold text-white mt-2">--</p>
                    <span class="text-xs text-indigo-400">Stored in DynamoDB</span>
                </div>
                <div class="bg-slate-800/80 p-5 rounded-xl border border-slate-700">
                    <span class="text-xs font-semibold text-slate-400 uppercase">Active Volumes</span>
                    <p class="text-3xl font-bold text-white mt-2">5 Volumes</p>
                    <span class="text-xs text-indigo-400">Multi-tenant Corp</span>
                </div>
            </div>

            <div class="bg-slate-800 p-6 rounded-xl border border-slate-700 mb-8">
                <h2 class="text-lg font-bold text-white mb-4">Disaster Recovery - Immediate Failover Trigger</h2>
                <div class="flex flex-wrap gap-4">
                    <button onclick="recoverVolume('vol-001')" class="bg-indigo-600 hover:bg-indigo-500 text-white font-medium px-4 py-2 rounded-lg text-sm transition">Recover vol-001</button>
                    <button onclick="recoverVolume('vol-002')" class="bg-indigo-600 hover:bg-indigo-500 text-white font-medium px-4 py-2 rounded-lg text-sm transition">Recover vol-002</button>
                    <button onclick="recoverVolume('vol-003')" class="bg-indigo-600 hover:bg-indigo-500 text-white font-medium px-4 py-2 rounded-lg text-sm transition">Recover vol-003</button>
                    <button onclick="recoverVolume('vol-004')" class="bg-indigo-600 hover:bg-indigo-500 text-white font-medium px-4 py-2 rounded-lg text-sm transition">Recover vol-004</button>
                    <button onclick="recoverVolume('vol-005')" class="bg-indigo-600 hover:bg-indigo-500 text-white font-medium px-4 py-2 rounded-lg text-sm transition">Recover vol-005</button>
                </div>
                <div id="recovery-alert" class="mt-4 p-4 rounded-lg hidden font-mono text-xs"></div>
            </div>

            <div class="bg-slate-800 rounded-xl border border-slate-700 overflow-hidden">
                <div class="p-4 border-b border-slate-700">
                    <h2 class="text-lg font-bold text-white">Live Orchestration Telemetry (DynamoDB Stream)</h2>
                </div>
                <div class="overflow-x-auto">
                    <table class="w-full text-left text-sm text-slate-300">
                        <thead class="bg-slate-900 text-xs uppercase text-slate-400 border-b border-slate-700">
                            <tr>
                                <th class="p-4">Snapshot ID</th>
                                <th class="p-4">Volume ID</th>
                                <th class="p-4">Workflow State</th>
                                <th class="p-4">Tier</th>
                                <th class="p-4">Size (MB)</th>
                                <th class="p-4">Checksum</th>
                                <th class="p-4">Latency (ms)</th>
                            </tr>
                        </thead>
                        <tbody id="telemetry-body"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <script>
            async function fetchMetrics() {
                try {
                    const res = await fetch('/telemetry');
                    const data = await res.json();
                    document.getElementById('rpo-val').innerText = data.calculated_rpo_seconds + ' s';
                    document.getElementById('rto-val').innerText = data.calculated_rto_seconds + ' s';
                    document.getElementById('count-val').innerText = data.total_snapshots_indexed;
                    const badge = document.getElementById('sla-badge');
                    badge.innerText = data.sla_status;
                    badge.className = data.sla_status === 'MEETING_SLA' 
                        ? 'px-4 py-2 rounded-full font-bold text-xs uppercase bg-emerald-950 text-emerald-400 border border-emerald-800'
                        : 'px-4 py-2 rounded-full font-bold text-xs uppercase bg-rose-950 text-rose-400 border border-rose-800';

                    const tbody = document.getElementById('telemetry-body');
                    tbody.innerHTML = '';
                    data.recent_events.forEach(ev => {
                        let stateColor = 'text-amber-400';
                        if (ev.state === 'COMPLETED') stateColor = 'text-emerald-400 font-semibold';
                        if (ev.state === 'TRIGGERED') stateColor = 'text-blue-400';

                        tbody.innerHTML += `
                            <tr class="border-b border-slate-700/50 hover:bg-slate-750">
                                <td class="p-4 font-mono text-xs text-indigo-300">${ev.snapshot_id}</td>
                                <td class="p-4 font-semibold">${ev.volume_id}</td>
                                <td class="p-4 ${stateColor}">${ev.state}</td>
                                <td class="p-4 text-xs font-mono">${ev.tier}</td>
                                <td class="p-4">${ev.size_mb} MB</td>
                                <td class="p-4 font-mono text-xs text-slate-400">${ev.checksum.substring(0, 12)}...</td>
                                <td class="p-4 font-mono text-xs">${ev.latency_ms} ms</td>
                            </tr>
                        `;
                    });
                } catch(err) {
                    console.error("Telemetry fetch failed", err);
                }
            }

            async function recoverVolume(volId) {
                const alertBox = document.getElementById('recovery-alert');
                alertBox.classList.remove('hidden', 'bg-emerald-950', 'bg-rose-950', 'text-emerald-300', 'text-rose-300', 'border');
                alertBox.innerText = 'Triggering automated failover for ' + volId + '...';
                try {
                    const res = await fetch('/recover/' + volId, { method: 'POST' });
                    const result = await res.json();
                    if(res.ok) {
                        alertBox.className = 'mt-4 p-4 rounded-lg font-mono text-xs bg-emerald-950 border border-emerald-700 text-emerald-300';
                        alertBox.innerHTML = `<strong>FAILOVER SUCCESSFUL:</strong> Restored ${volId} from ${result.restored_from_tier} in <strong>${result.rto_latency_ms} ms</strong> (RTO). SHA-256 Checksum Verified: ${result.checksum_verified}.`;
                    } else {
                        alertBox.className = 'mt-4 p-4 rounded-lg font-mono text-xs bg-rose-950 border border-rose-700 text-rose-300';
                        alertBox.innerText = result.detail || 'Recovery failed.';
                    }
                } catch(err) {
                    alertBox.className = 'mt-4 p-4 rounded-lg font-mono text-xs bg-rose-950 border border-rose-700 text-rose-300';
                    alertBox.innerText = 'Network error executing recovery routine.';
                }
            }

            fetchMetrics();
        </script>
    </body>
    </html>
    """