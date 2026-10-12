KoraGT Unified Engine
KoraGT is a cloud-agnostic, cost-efficient server panel and compliance-hardened automation suite. It acts as a unified control plane that merges real-time financial telemetry (FinOps) with strict NIST SP 800-190 container security auditing, eliminating operational blind spots and hidden cloud costs.

Core Features
Asynchronous Control Plane: Utilizes Redis and BullMQ to offload heavy container orchestration commands, ensuring the Express REST API remains non-blocking and highly responsive.

NIST SP 800-190 Compliance Auditor: Functions as a strict admission controller for persistent servers. It automatically rejects container manifests that attempt to run as root, lack read-only filesystems, omit default Seccomp profiles, or fail to drop kernel capabilities (--cap-drop=ALL).

Live FinOps Telemetry: Interfaces directly with Docker and cgroups v2 to map raw CPU delta percentages and memory footprints to real-time hourly burn rates.

Automated Cost-Kill Switch: Autonomously suspends idle containers operating below user-defined CPU thresholds (default 2.0%), ensuring unused resources do not incur unnecessary cloud provider fees.

UI Data Mapping: Exposes a perfectly formatted JSON array directly mapping to the Angular frontend dashboard for seamless integration of metrics and lifecycle states.

Prerequisites
Node.js: v18.0.0 or higher.

Docker: Engine running on a host with cgroups v2 enabled. The user running KoraGT must have read/write access to /var/run/docker.sock.

Redis: A running Redis instance (local or remote) to handle the BullMQ job queues.

Installation & Setup
Clone the repository and navigate to the root directory.

Install the required Node.js dependencies defined in package.json:

Bash
npm install
Ensure your Redis server is running on the default port (6379), or set the REDIS_HOST environment variable.

Start the KoraGT Unified Engine:

Bash
npm start
The engine will initialize and listen on port 3000 by default.

API Reference
GET /api/containers
Fetches a list of all containers across the host, calculates their real-time CPU/memory deltas, maps them to an hourly financial burn rate, and returns the data structure required by the KoraGT dashboard.

POST /api/containers/deploy
Accepts a JSON Docker manifest in the request body. Routes the manifest through the NISTAuditor. If the manifest passes all security checks (Scorecard 100/100), it queues the deployment job in Redis and returns an HTTP 202 status with a Job ID.

POST /api/containers/:id/action
Accepts a JSON body containing a lifecycle action (e.g., { "action": "STOP" }). Adds the action to the asynchronous worker queue to prevent event-loop blocking and returns a Job ID for frontend tracking.
