const express = require('express');
const Docker = require('dockerode');
const { Queue, Worker } = require('bullmq');
const path = require('path');

// ============================================================================
// 0. HOST HARDWARE PROFILE (Baseline Reference for Power Scaling)
// ============================================================================
const HOST_HARDWARE_PROFILE = {
    machineName: 'Central-Minnesota-Custom-Rig',
    totalCores: 8,
    maxTdpWatts: 125.0,         // Maximum Thermal Design Power (TDP) wattage for the processor
    kwhCostUsd: 0.12             // Local electricity rate ($0.12 per kWh)
};

// ============================================================================
// 1. MODULE: ENHANCED NIST SP 800-190 AUDITOR (Full-Time Server Policies)
// ============================================================================
class NISTAuditor {
    static auditManifest(config) {
        const auditLog = [];
        let passed = true;

        if (!config.User || config.User === '0' || config.User === 'root') {
            auditLog.push({ check: 'USER_EXECUTION', status: 'FAIL', detail: 'Root execution prohibited.' });
            passed = false;
        } else {
            auditLog.push({ check: 'USER_EXECUTION', status: 'PASS', detail: 'Non-root user verified.' });
        }

        if (!config.HostConfig?.ReadonlyRootfs) {
            auditLog.push({ check: 'READONLY_ROOTFS', status: 'FAIL', detail: 'Read-write root filesystem detected.' });
            passed = false;
        } else {
            auditLog.push({ check: 'READONLY_ROOTFS', status: 'PASS', detail: 'Readonly rootfs enabled.' });
        }

        if (!config.HostConfig?.CapDrop?.includes('ALL')) {
            auditLog.push({ check: 'CAPABILITY_STRIPPING', status: 'FAIL', detail: '--cap-drop=ALL missing.' });
            passed = false;
        } else {
            auditLog.push({ check: 'CAPABILITY_STRIPPING', status: 'PASS', detail: 'Capabilities stripped successfully.' });
        }

        const restartPolicy = config.HostConfig?.RestartPolicy?.Name;
        if (!['always', 'unless-stopped'].includes(restartPolicy)) {
            auditLog.push({ check: 'RESTART_POLICY', status: 'FAIL', detail: 'Must use "always" or "unless-stopped" for persistent server resilience.' });
            passed = false;
        } else {
            auditLog.push({ check: 'RESTART_POLICY', status: 'PASS', detail: 'Valid restart policy set.' });
        }

        const secOpts = config.HostConfig?.SecurityOpt || [];
        const hasSeccomp = secOpts.some(opt => opt.startsWith('seccomp='));
        if (!hasSeccomp) {
            auditLog.push({ check: 'SECCOMP_PROFILE', status: 'FAIL', detail: 'Default Seccomp profile missing.' });
            passed = false;
        } else {
            auditLog.push({ check: 'SECCOMP_PROFILE', status: 'PASS', detail: 'Seccomp profile verified.' });
        }

        return { approved: passed, auditLog };
    }
}

// ============================================================================
// 2. MODULE: FINOPS & PROGRAM-LEVEL POWER MAPPING ENGINE
// ============================================================================
class FinOpsEngine {
    constructor(docker, hardwareProfile) {
        this.docker = docker;
        this.hardware = hardwareProfile;
    }

    // Calculates power purely based on the individual container/program's CPU usage ratio
    calculateProgramPower(cpuPercent, activePlayers = 0) {
        // Normalize cpuPercent (e.g., 6.4%) across total system cores
        const normalizedCpuRatio = Math.max(cpuPercent, 0) / 100.0;
        
        // Program power draw is its direct proportional slice of the processor's TDP + player auxiliary load
        const programWatts = (normalizedCpuRatio * this.hardware.maxTdpWatts) + (activePlayers * 0.5);
        const hourlyCost = (programWatts / 1000.0) * this.hardware.kwhCostUsd;

        return {
            power_draw_watts: parseFloat(programWatts.toFixed(2)),
            burn_rate_per_hour: parseFloat(hourlyCost.toFixed(5)),
            hardware_reference: this.hardware.machineName
        };
    }

    async getDashboardMetrics() {
        const containers = await this.docker.listContainers({ all: true });
        const dashboardData = [];

        for (const c of containers) {
            const container = this.docker.getContainer(c.Id);
            let cpuUsagePct = 0.0;
            let memoryUsageMb = 0.0;

            if (c.State === 'running') {
                try {
                    const stats = await container.stats({ stream: false });
                    const cpuDelta = stats.cpu_stats.cpu_usage.total_usage - stats.precpu_stats.cpu_usage.total_usage;
                    const systemDelta = stats.cpu_stats.system_cpu_usage - stats.precpu_stats.system_cpu_usage;
                    const numberCpus = stats.cpu_stats.online_cpus || this.hardware.totalCores;
                    if (systemDelta > 0.0 && cpuDelta > 0.0) {
                        cpuUsagePct = (cpuDelta / systemDelta) * numberCpus * 100.0;
                    }
                    memoryUsageMb = (stats.memory_stats.usage || 0) / (1024 * 1024);
                } catch (e) {
                    // Fallback if stats fail temporarily
                }
            }

            const powerMetrics = this.calculateProgramPower(cpuUsagePct, 0);

            dashboardData.push({
                name: c.Names[0].replace('/', ''),
                image: c.Image,
                id: c.Id.substring(0, 8),
                fullId: c.Id,
                status: c.State,
                cpu: parseFloat(cpuUsagePct.toFixed(1)),
                memory: parseFloat(memoryUsageMb.toFixed(0)),
                power_draw_watts: powerMetrics.power_draw_watts,
                burnRate: powerMetrics.burn_rate_per_hour
            });
        }
        return dashboardData;
    }
}

// ============================================================================
// 3. CORE APPLICATION ROUTING & QUEUE
// ============================================================================
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname)));

const isWindows = process.platform === 'win32';
const docker = new Docker(
  isWindows 
    ? { socketPath: '//./pipe/docker_engine' } 
    : { socketPath: '/var/run/docker.sock' }
);

const finOps = new FinOpsEngine(docker, HOST_HARDWARE_PROFILE);
const connection = { host: process.env.REDIS_HOST || '127.0.0.1', port: 6379 };

const containerQueue = new Queue('container-ops', { connection });
const worker = new Worker('container-ops', async (job) => {
    const { action, containerId, config } = job.data;
    if (action === 'DEPLOY') {
        const container = await docker.createContainer(config);
        await container.start();
        return { containerId: container.id, status: 'RUNNING' };
    }
    const container = docker.getContainer(containerId);
    if (action === 'START') await container.start();
    if (action === 'STOP') await container.stop();
    if (action === 'RESTART') await container.restart();
    return { containerId, status: action + '_COMPLETED' };
}, { connection });

// API: List Containers & Metrics
app.get('/api/containers', async (req, res) => {
    try {
        const metrics = await finOps.getDashboardMetrics();
        res.json(metrics);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// API: Container Lifecycle Actions
app.post('/api/containers/:id/action', async (req, res) => {
    const { action } = req.body;
    const job = await containerQueue.add('lifecycle', { action: action.toUpperCase(), containerId: req.params.id });
    res.status(202).json({ message: `Action ${action} queued`, jobId: job.id });
});

// API: NIST Audit & Deploy
app.post('/api/containers/deploy', async (req, res) => {
    const auditResult = NISTAuditor.auditManifest(req.body);
    if (!auditResult.approved) {
        return res.status(422).json({ error: 'Security Audit Failed', log: auditResult.auditLog });
    }
    const job = await containerQueue.add('deploy', { action: 'DEPLOY', config: req.body });
    res.status(202).json({ message: 'Deployment queued', jobId: job.id, auditLog: auditResult.auditLog });
});

// API: Job Queue Status
app.get('/api/jobs', async (req, res) => {
    try {
        const waiting = await containerQueue.getWaiting();
        const active = await containerQueue.getActive();
        const completed = await containerQueue.getCompleted();
        const failed = await containerQueue.getFailed();

        const formatJob = (j) => ({ id: j.id, name: j.name, data: j.data, timestamp: j.timestamp });

        res.json({
            counts: { waiting: waiting.length, active: active.length, completed: completed.length, failed: failed.length },
            jobs: [...active, ...waiting, ...completed.slice(0, 10)].map(formatJob)
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// API: Container Logs Stream
app.get('/api/containers/:id/logs', async (req, res) => {
    try {
        const container = docker.getContainer(req.params.id);
        const logs = await container.logs({
            stdout: true,
            stderr: true,
            tail: 100,
            timestamps: true
        });
        res.send(logs.toString('utf8'));
    } catch (err) {
        res.status(500).send(`Error retrieving logs: ${err.message}`);
    }
});

// ============================================================================
// 4. SPECIALIZED WORKLOAD: MINECRAFT INTEGRATION ENDPOINTS
// ============================================================================

// A. Deploy & Audit Minecraft Container via NIST Auditor
app.post('/api/minecraft/deploy', async (req, res) => {
    const auditResult = NISTAuditor.auditManifest(req.body);
    if (!auditResult.approved) {
        return res.status(422).json({
            status: 'Audit Failed',
            code: 422,
            auditLog: auditResult.auditLog
        });
    }
    try {
        const container = await docker.createContainer(req.body);
        await container.start();

        const logStream = await container.logs({
            follow: true,
            stdout: true,
            stderr: true,
            timestamps: true
        });

        logStream.on('data', (chunk) => {
            const logLine = chunk.toString('utf8');
            if (logLine.includes('joined the game')) {
                console.log(`[MINECRAFT EVENT] Player Connected: ${logLine}`);
            }
        });

        res.status(201).json({ message: 'Minecraft instance secured and running.', id: container.id, auditLog: auditResult.auditLog });
    } catch (err) {
        res.status(500).json({ error: 'Deployment execution failed.', details: err.message });
    }
});

// B. Dispatch Live Console Commands into Minecraft Terminal (stdin)
app.post('/api/minecraft/:id/command', async (req, res) => {
    const { command } = req.body;
    try {
        const container = docker.getContainer(req.params.id);
        const attachStream = await container.attach({ stream: true, stdin: true });
        
        attachStream.write(command + '\n');
        attachStream.end();

        res.status(200).json({ status: 'Command dispatched successfully' });
    } catch (err) {
        res.status(500).json({ error: 'Failed to dispatch command to Minecraft terminal.' });
    }
});

// C. Program-Level FinOps Energy Telemetry & Player Analytics for Minecraft
app.get('/api/minecraft/:id/telemetry', async (req, res) => {
    try {
        const container = docker.getContainer(req.params.id);
        const stats = await container.stats({ stream: false });

        const cpuDelta = stats.cpu_stats.cpu_usage.total_usage - stats.precpu_stats.cpu_usage.total_usage;
        const systemDelta = stats.cpu_stats.system_cpu_usage - stats.precpu_stats.system_cpu_usage;
        const numberCpus = stats.cpu_stats.online_cpus || HOST_HARDWARE_PROFILE.totalCores;
        
        let cpuPercent = 0.0;
        if (systemDelta > 0.0 && cpuDelta > 0.0) {
            cpuPercent = (cpuDelta / systemDelta) * numberCpus * 100.0;
        }

        const memUsageGB = (stats.memory_stats.usage / (1024 * 1024 * 1024)).toFixed(2);
        const activePlayers = parseInt(req.query.players || 0); 

        // Calculates power based directly on this specific container/program's CPU usage
        const powerMetrics = finOps.calculateProgramPower(cpuPercent, activePlayers);

        res.status(200).json({
            hardware_reference: HOST_HARDWARE_PROFILE.machineName,
            metrics: {
                cpu_percent: parseFloat(cpuPercent.toFixed(2)),
                memory_gb: parseFloat(memUsageGB),
                active_players: activePlayers
            },
            finops: powerMetrics
        });
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch program-level telemetry for Minecraft.' });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`KoraGT Unified Engine Active on Port [Program-Level FinOps Enabled]`));
