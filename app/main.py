import time
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from app.aws_client import TABLE_NAME, dynamodb_client, init_aws_infrastructure
from app.models import RecoveryResponse, SnapshotLog, TelemetryResponse
from app.orchestrator import VOLUMES_CATALOG, OrchestratorEngine

scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Initialize AWS mock infrastructure
    init_aws_infrastructure()
    
    # 2. Run initial snapshot cycle immediately on startup
    OrchestratorEngine.execute_fleet_backup()
    
    # 3. Schedule regular snapshot jobs every 25 seconds
    scheduler.add_job(
        OrchestratorEngine.execute_fleet_backup,
        "interval",
        seconds=25,
        id="automated_dr_sync",
    )
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(
    title="Enterprise Cloud Storage & DR Orchestrator",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def get_system_health():
    """Health check endpoint showing volume catalogs and scheduler health."""
    return {
        "status": "OPERATIONAL",
        "active_volumes": list(VOLUMES_CATALOG.keys()),
        "scheduler_active": scheduler.running,
        "timestamp": time.time(),
    }


@app.get("/telemetry", response_model=TelemetryResponse)
def get_telemetry_metrics():
    """Returns indexed snapshot history and calculated RPO/RTO metrics from DynamoDB."""
    response = dynamodb_client.scan(TableName=TABLE_NAME, Limit=60)
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

    # Sort descending by timestamp
    events.sort(key=lambda x: x.timestamp, reverse=True)

    # RPO Calculation (average delta between completed backup runs)
    completed_timestamps = sorted([e.timestamp for e in events if e.state == "COMPLETED"])
    calculated_rpo = 25.0
    if len(completed_timestamps) > 1:
        deltas = [
            t2 - t1
            for t1, t2 in zip(completed_timestamps[:-1], completed_timestamps[1:])
        ]
        calculated_rpo = sum(deltas) / len(deltas)

    # RTO Calculation (average latency to verify and persist a snapshot)
    completed_latencies = [e.latency_ms for e in events if e.state == "COMPLETED"]
    calculated_rto = (sum(completed_latencies) / len(completed_latencies) / 1000) if completed_latencies else 0.15

    sla_status = "MEETING_SLA" if calculated_rpo <= 60.0 else "SLA_BREACHED"

    return TelemetryResponse(
        active_tenant="tenant-corp",
        calculated_rpo_seconds=round(calculated_rpo, 2),
        calculated_rto_seconds=round(calculated_rto, 2),
        total_snapshots_indexed=len(events),
        sla_status=sla_status,
        recent_events=events[:15],
    )


@app.post("/recover/{volume_id}", response_model=RecoveryResponse)
def trigger_volume_recovery(volume_id: str):
    """Triggers an automated disaster recovery failover restore routine."""
    if volume_id not in VOLUMES_CATALOG:
        raise HTTPException(
            status_code=404,
            detail=f"Volume '{volume_id}' is not an active enterprise volume.",
        )
    try:
        recovery_result = OrchestratorEngine.run_failover_recovery(volume_id)
        return RecoveryResponse(**recovery_result)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"DR Failover error: {str(ex)}")


@app.get("/dashboard", response_class=HTMLResponse)
def serve_dashboard():
    """Real-time HTML/JS operations dashboard with live polling and DR trigger buttons."""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Enterprise DR Orchestrator Console</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-slate-900 text-slate-100 font-sans p-6">
        <div class="max-w-7xl mx-auto space-y-6">
            <header class="flex justify-between items-center border-b border-slate-700 pb-4">
                <div>
                    <h1 class="text-2xl font-bold tracking-tight text-cyan-400">Enterprise Cloud DR Orchestrator</h1>
                    <p class="text-xs text-slate-400 mt-1">AWS Multi-Tier Replication & Disaster Recovery Monitor</p>
                </div>
                <div id="sla-badge" class="px-3 py-1 bg-green-900 text-green-300 rounded text-xs font-semibold uppercase">
                    Monitoring SLAs
                </div>
            </header>

            <!-- Metrics Overview Cards -->
            <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div class="bg-slate-800 p-4 rounded-lg border border-slate-700">
                    <p class="text-xs text-slate-400 uppercase">Calculated RPO</p>
                    <p id="rpo-val" class="text-2xl font-bold text-white mt-1">-- s</p>
                    <span class="text-xs text-slate-400">Target: &lt; 60s</span>
                </div>
                <div class="bg-slate-800 p-4 rounded-lg border border-slate-700">
                    <p class="text-xs text-slate-400 uppercase">Calculated RTO</p>
                    <p id="rto-val" class="text-2xl font-bold text-white mt-1">-- s</p>
                    <span class="text-xs text-slate-400">Target: &lt; 2s</span>
                </div>
                <div class="bg-slate-800 p-4 rounded-lg border border-slate-700">
                    <p class="text-xs text-slate-400 uppercase">Indexed State Transitions</p>
                    <p id="total-val" class="text-2xl font-bold text-cyan-400 mt-1">0</p>
                    <span class="text-xs text-slate-400">Stored in DynamoDB</span>
                </div>
                <div class="bg-slate-800 p-4 rounded-lg border border-slate-700">
                    <p class="text-xs text-slate-400 uppercase">Active Volumes</p>
                    <p class="text-2xl font-bold text-indigo-400 mt-1">5 Volumes</p>
                    <span class="text-xs text-slate-400">Multi-tenant Corp</span>
                </div>
            </div>

            <!-- Volume Recovery Trigger Section -->
            <div class="bg-slate-800 p-5 rounded-lg border border-slate-700">
                <h2 class="text-sm font-semibold uppercase text-slate-300 mb-3">Disaster Recovery - Immediate Failover Trigger</h2>
                <div class="flex flex-wrap gap-3">
                    <button onclick="triggerRecovery('vol-001')" class="px-3 py-1.5 bg-blue-600 hover:bg-blue-500 rounded text-xs font-medium">Recover vol-001</button>
                    <button onclick="triggerRecovery('vol-002')" class="px-3 py-1.5 bg-blue-600 hover:bg-blue-500 rounded text-xs font-medium">Recover vol-002</button>
                    <button onclick="triggerRecovery('vol-003')" class="px-3 py-1.5 bg-blue-600 hover:bg-blue-500 rounded text-xs font-medium">Recover vol-003</button>
                    <button onclick="triggerRecovery('vol-004')" class="px-3 py-1.5 bg-blue-600 hover:bg-blue-500 rounded text-xs font-medium">Recover vol-004</button>
                    <button onclick="triggerRecovery('vol-005')" class="px-3 py-1.5 bg-blue-600 hover:bg-blue-500 rounded text-xs font-medium">Recover vol-005</button>
                </div>
                <div id="recovery-output" class="mt-3 text-xs font-mono text-emerald-400 bg-slate-950 p-3 rounded hidden"></div>
            </div>

            <!-- Real-Time Telemetry Stream -->
            <div class="bg-slate-800 rounded-lg border border-slate-700 overflow-hidden">
                <div class="p-4 border-b border-slate-700">
                    <h2 class="text-sm font-semibold uppercase text-slate-300">Live Orchestration Telemetry (DynamoDB Stream)</h2>
                </div>
                <div class="overflow-x-auto">
                    <table class="w-full text-left text-xs text-slate-300">
                        <thead class="bg-slate-900 text-slate-400 uppercase text-[10px]">
                            <tr>
                                <th class="p-3">Snapshot ID</th>
                                <th class="p-3">Volume ID</th>
                                <th class="p-3">Workflow State</th>
                                <th class="p-3">Tier</th>
                                <th class="p-3">Size (MB)</th>
                                <th class="p-3">Checksum</th>
                                <th class="p-3">Latency (ms)</th>
                            </tr>
                        </thead>
                        <tbody id="telemetry-table-body" class="divide-y divide-slate-700 font-mono">
                            <!-- Injected dynamically via polling -->
                        </tbody>
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
                    document.getElementById('total-val').innerText = data.total_snapshots_indexed;

                    const slaBadge = document.getElementById('sla-badge');
                    slaBadge.innerText = data.sla_status;
                    slaBadge.className = data.sla_status === 'MEETING_SLA' 
                        ? 'px-3 py-1 bg-green-900 text-green-300 rounded text-xs font-semibold uppercase'
                        : 'px-3 py-1 bg-red-900 text-red-300 rounded text-xs font-semibold uppercase';

                    const tableBody = document.getElementById('telemetry-table-body');
                    tableBody.innerHTML = '';
                    data.recent_events.forEach(ev => {
                        const tr = document.createElement('tr');
                        tr.className = 'hover:bg-slate-750 transition-colors';
                        tr.innerHTML = `
                            <td class="p-3 text-cyan-400">${ev.snapshot_id}</td>
                            <td class="p-3">${ev.volume_id}</td>
                            <td class="p-3"><span class="px-2 py-0.5 rounded text-[10px] bg-slate-700 text-slate-200">${ev.state}</span></td>
                            <td class="p-3"><span class="px-2 py-0.5 rounded text-[10px] ${ev.tier === 'HOT' ? 'bg-amber-900 text-amber-300' : 'bg-cyan-950 text-cyan-300'}">${ev.tier}</span></td>
                            <td class="p-3">${ev.size_mb} MB</td>
                            <td class="p-3 truncate max-w-[120px]" title="${ev.checksum}">${ev.checksum.substring(0, 12)}...</td>
                            <td class="p-3">${ev.latency_ms.toFixed(1)} ms</td>
                        `;
                        tableBody.appendChild(tr);
                    });
                } catch(err) {
                    console.error("Telemetry update error:", err);
                }
            }

            async function triggerRecovery(volId) {
                const out = document.getElementById('recovery-output');
                out.classList.remove('hidden');
                out.innerText = `[Executing DR Routine] Requesting failover for ${volId}...`;
                try {
                    const res = await fetch(`/recover/${volId}`, { method: 'POST' });
                    const result = await res.json();
                    out.innerText = JSON.stringify(result, null, 2);
                } catch (err) {
                    out.innerText = `Error triggering recovery: ${err.message}`;
                }
            }

            setInterval(fetchMetrics, 3000);
            fetchMetrics();
        </script>
    </body>
    </html>
    """