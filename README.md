# TATKAL-X

> **Repository note:** This repository is a research/simulation prototype for reservation congestion, retry dynamics, predictive control, and event-stream analytics. It is not a live railway booking integration.

## Important source files

- `core2_kafka.py` — simulation engine, workload/retry model, reservation/queue dynamics, MPC/CBF controller.
- `app_kafka.py` and `index.html` — live Flask UI/API.
- `event_schema.py` and `kafka_producer.py` — structured simulation events and optional Kafka publishing.
- `stream_processorv1.py` — Kafka consumer for rolling metrics and anomaly indicators.
- `stream_storage.py` — Kafka consumer that persists event history to SQLite.
- `analytics_api.py` — read-only analytics API and browser dashboard.
- `selftest.py` — self-test / validation helper.
- `requirements.txt` — Python dependencies.
- `Dockerfile` and `docker-compose*.yml` — container/deployment configuration; check the commands and filenames against the actual files in this repository before using the full stack.

## Before publishing

- Do not commit `.venv/`, secrets, local databases, or generated run data.
- The SQLite database is intentionally excluded; it can be recreated when the storage consumer runs.
- Check `docker-compose.deploy.yml`: the supplied configuration may reference alternate filenames such as `stream_processor.py` or `analytics_api_interactive.py`, while this archive contains `stream_processorv1.py` and `analytics_api.py`. Align those commands before deploying the Compose stack.

---

## Finite-Horizon Reservation Dynamics with Real-Time Streaming and Analytics

TATKAL-X is a research-oriented reservation dynamics framework designed
to study synchronized flash-crowd reservation systems under
finite-horizon demand, endogenous retry pressure, queue/service
dynamics, inventory constraints, and safety-aware predictive control.

The research core models the reservation dynamics. A supporting
real-time data engineering layer captures simulation events, streams
them through Apache Kafka, performs online stream analytics, persists
event data, and exposes an interactive analytics interface.

> **Important:** Kafka, stream processing, persistent storage, and
> deployment infrastructure are supporting layers. They do not replace
> or alter the core TATKAL-X research contribution.

------------------------------------------------------------------------

## 1. Research Objective

TATKAL-X studies reservation systems subjected to synchronized,
high-intensity demand such as flash-sale or Tatkal-like booking windows.

The framework combines:

-   Finite-horizon reservation dynamics
-   Synchronized flash-crowd arrivals
-   Endogenous retry / Hawkes-style pressure
-   FCFS queue and service dynamics
-   Reservation and payment/inventory dynamics
-   Predictive control using Model Predictive Control (MPC)
-   Constraint enforcement using a Control Barrier Function (CBF)
-   Real-time event streaming
-   Online stream analytics
-   Persistent event storage
-   Interactive post-run analytics

The infrastructure layer makes the research system observable and
deployable without changing the underlying mathematical/control logic.

------------------------------------------------------------------------

# 2. System Architecture

``` text
                         TATKAL-X RESEARCH CORE
                                  |
                                  | simulation events
                                  v
                         +----------------------+
                         |   Event Schema       |
                         |   event_schema.py    |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         |   Apache Kafka        |
                         |   Topic:              |
                         |   tatkal-x-events     |
                         +----------+-----------+
                                    |
                  +-----------------+------------------+
                  |                                    |
                  v                                    v
       +----------------------+             +----------------------+
       | Stream Processor     |             | Persistent Storage   |
       | stream_processor.py  |             | stream_storage.py    |
       |                      |             |                      |
       | Delta rates          |             | SQLite               |
       | Rolling statistics   |             | Event history        |
       | Anomaly detection    |             | Raw event retention  |
       +----------------------+             +----------+-----------+
                                                        |
                                                        v
                                             +----------------------+
                                             | Interactive Analytics|
                                             | analytics_api_       |
                                             | interactive.py       |
                                             +----------+-----------+
                                                        |
                                                        v
                                             http://127.0.0.1:5001
```

------------------------------------------------------------------------

# 3. Architecture Layers

## Layer 1 --- Structured Event Schema

File:

``` text
event_schema.py
```

The simulation output is converted into a consistent event
representation.

The event contains the following simulation fields:

``` text
t
pressure
active
inventory
attempts
initial_attempts
retry_attempts
activated
completed
pending
service_rate
robust_service_rate
safe_concurrency
latency_admission_capacity
service_time_bound_s
backend_workload_s
credits
arrival_rate
admission_rate
cpu_pct
rss_mb
cbf_headroom
cbf_filtered
safe_absorption
mpc_objective
safety_violation
```

The event also carries metadata such as:

``` text
simulation_id
controller_mode
event_type
schema_version
```

The event schema is intentionally separated from the research
mathematics so that downstream infrastructure can consume simulation
output consistently.

------------------------------------------------------------------------

# 4. Layer 2 --- Kafka Event Streaming

Files:

``` text
kafka_producer.py
```

Kafka is used as the real-time event backbone.

Default configuration:

``` text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=tatkal-x-events
```

The producer is failure-tolerant:

-   Kafka can be disabled for research benchmarks/self-tests.
-   The simulation does not depend on Kafka for its mathematical
    execution.
-   Kafka connection failures do not replace or modify the research
    calculations.
-   Events are serialized as JSON.

Kafka producer configuration uses:

``` text
acks=all
retries=3
linger_ms=5
```

The live event flow is:

``` text
Engine.tick()
    |
    v
event_schema.build_event()
    |
    v
KafkaEventProducer.publish()
    |
    v
tatkal-x-events
```

------------------------------------------------------------------------

# 5. Layer 3 --- Real-Time Stream Processing

File:

``` text
stream_processor.py
```

The stream processor consumes events from:

``` text
tatkal-x-events
```

It calculates online metrics without treating cumulative counters as
instantaneous rates.

### Delta-rate metrics

For cumulative variables:

``` text
completion_rate_per_s = Δcompleted / Δt

retry_rate_per_s = Δretry_attempts / Δt

attempt_rate_per_s = Δattempts / Δt

activation_rate_per_s = Δactivated / Δt
```

### Rolling state metrics

Rolling statistics are maintained for variables such as:

-   Pressure
-   Active users
-   Arrival rate
-   Admission rate
-   CBF headroom
-   Safe concurrency
-   Backend workload
-   CPU utilization
-   MPC objective

### Online anomaly indicators

The processor checks indicators including:

``` text
concurrency_pressure
high_pressure
low_cbf_headroom
safety_violation
```

It also reports retry amplification:

``` text
retry amplification =
retry rate / attempt rate
```

The stream processor is intended for operational observation of the live
simulation stream. It does not modify the simulation controller.

------------------------------------------------------------------------

# 6. Layer 4 --- Persistent Event Storage

File:

``` text
stream_storage.py
```

A lightweight SQLite store is used to persist Kafka events.

Database:

``` text
tatkal_x_stream.db
```

Table:

``` text
tatkal_x_events
```

The database stores:

-   Simulation identifier
-   Controller mode
-   All event fields
-   Raw JSON event
-   Storage timestamp

Indexes are maintained for commonly queried fields such as:

``` text
simulation_id
t
controller_mode
```

SQLite was selected intentionally to keep the deployment lightweight. A
separate database server is not required for the current research
implementation.

------------------------------------------------------------------------

# 7. Layer 5 --- Interactive Analytics

File:

``` text
analytics_api_interactive.py
```

The analytics application provides an interactive browser-based
interface.

Default address:

``` text
http://127.0.0.1:5001
```

The interface provides:

-   Simulation selection
-   Controller selection
-   Metric selection
-   Interactive time-series charts
-   Clickable chart points
-   KPI cards
-   Controller comparison
-   Event explorer
-   Refresh functionality

## API endpoints

### Health

``` text
GET /health
```

### Overall analytics summary

``` text
GET /api/analytics/summary
```

### Controller comparison

``` text
GET /api/analytics/controllers
```

### Simulation analytics

``` text
GET /api/analytics/simulation/<simulation_id>
```

### Event explorer

``` text
GET /api/analytics/events
```

The event explorer supports filtering by:

``` text
simulation_id
controller_mode
limit
offset
```

------------------------------------------------------------------------

# 8. Layer 6 --- Docker Deployment

Deployment uses:

``` text
Docker
Docker Compose
```

No Kubernetes cluster is required.

Deployment files:

``` text
Dockerfile
docker-compose.deploy.yml
.dockerignore
```

The deployment stack contains four services:

``` text
kafka
stream_processor
stream_storage
analytics
```

## Deployment architecture

``` text
                  Docker Compose Network
                         |
       +-----------------+-----------------+
       |                 |                 |
       v                 v                 v
    Kafka         Stream Processor    Stream Storage
       |                 |                 |
       |                 |                 v
       |                 |          SQLite volume
       |                 |                 |
       +-----------------+-----------------+
                         |
                         v
                     Analytics
                         |
                         v
                  Port 5001 on host
```

------------------------------------------------------------------------

# 9. Kafka Listener Configuration

The deployment uses two Kafka listeners.

### Container-to-container

``` text
kafka:9092
```

### Host-to-container

``` text
localhost:29092
```

This allows:

``` text
Docker services
    -> kafka:9092
```

while a TATKAL-X simulation running directly on the host can use:

``` text
localhost:29092
```

This avoids Kafka advertised-listener hostname problems between Docker
and the Windows host.

------------------------------------------------------------------------

# 10. Requirements

Python dependencies are defined in:

``` text
requirements.txt
```

Current requirements:

``` text
Flask>=3.0,<4
numpy>=2.0,<3
psutil>=6.0,<7
kafka-python>=2.2,<3
```

Python 3.11 is used by the deployment Docker image.

------------------------------------------------------------------------

# 11. Project Structure

A deployment-ready project should contain the following relevant files:

``` text
TATKAL-X/
│
├── app.py
├── core.py
├── core2_kafka.py
├── results.py
├── selftest.py
├── index.html
│
├── event_schema.py
├── kafka_producer.py
├── stream_processor.py
├── stream_storage.py
├── analytics_api_interactive.py
│
├── requirements.txt
│
├── Dockerfile
├── docker-compose.deploy.yml
├── docker-compose.kafka.yml
├── .dockerignore
│
├── tatkal_x_stream.db              # local runtime database, if present
│
├── results/
├── raw_runs/
│
└── documentation / research files
```

The exact repository may contain additional research documentation,
verification files, datasets, and generated result artifacts.

------------------------------------------------------------------------

# 12. Local Python Setup

Create a virtual environment:

### Windows PowerShell

``` powershell
py -3.11 -m venv .venv
```

Activate it:

``` powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

``` powershell
pip install -r requirements.txt
```

Verify Kafka Python support:

``` powershell
python -c "import kafka; print('kafka-python OK')"
```

------------------------------------------------------------------------

# 13. Running Kafka Locally with Docker

For Kafka-only deployment:

``` powershell
docker compose -f docker-compose.kafka.yml up -d
```

Check:

``` powershell
docker compose -f docker-compose.kafka.yml ps
```

Stop Kafka:

``` powershell
docker compose -f docker-compose.kafka.yml down
```

------------------------------------------------------------------------

# 14. Running the TATKAL-X Live Simulation

The research application can continue to run directly on the host.

When Kafka integration is enabled, configure the host-side Kafka
connection as:

``` text
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC=tatkal-x-events
```

The research core can also run with Kafka disabled. This is useful for:

-   Unit/self tests
-   Baseline experiments
-   Reproducibility checks
-   Performance benchmarking
-   Research runs where streaming infrastructure is unnecessary

The Kafka layer should not be treated as a dependency of the
mathematical research model.

------------------------------------------------------------------------

# 15. Running the Stream Processor

With Kafka available:

``` powershell
python stream_processor.py
```

Expected output resembles:

``` text
Simulation tatkal_x_8
Controller fixed_rate
Source t 23.6
Window 20
Rolling Pressure 0.6130
Rolling Active 33.50
Arrival Rate 0.50/s
Admission Rate 1.50/s
Completion Rate 10.00/s
Retry Rate 10.00/s
Attempt Rate 10.00/s
Activation Rate 0.00/s
Backend Workload 0.6201s
Safe Concurrency 180
CBF Headroom 180
Safety Violations 0
CBF Filtered 0
Retry Amplif 1.0003x
Concurrency Risk False
High Pressure False
Low CBF Headroom False
STREAM ANOMALY False
```

Values depend on the live simulation.

------------------------------------------------------------------------

# 16. Running Persistent Storage

Start:

``` powershell
python stream_storage.py
```

The process consumes Kafka events and stores them in:

``` text
tatkal_x_stream.db
```

Example:

``` text
[STORED] simulation=tatkal_x_8 t=23.6 total=935
```

The consumer uses an independent Kafka consumer group so that storage
does not interfere with the stream analytics consumer.

------------------------------------------------------------------------

# 17. Running Interactive Analytics

Start:

``` powershell
python analytics_api_interactive.py
```

Open:

``` text
http://127.0.0.1:5001
```

The application reads from:

``` text
tatkal_x_stream.db
```

------------------------------------------------------------------------

# 18. Full Docker Deployment

Make sure these files are in the project root:

``` text
Dockerfile
docker-compose.deploy.yml
.dockerignore
requirements.txt
```

Build all images:

``` powershell
docker compose -f docker-compose.deploy.yml build
```

Start the stack:

``` powershell
docker compose -f docker-compose.deploy.yml up -d
```

Check service status:

``` powershell
docker compose -f docker-compose.deploy.yml ps
```

Expected services:

``` text
tatkal-x-kafka
tatkal-x-stream-processor
tatkal-x-stream-storage
tatkal-x-analytics
```

Open the analytics interface:

``` text
http://127.0.0.1:5001
```

------------------------------------------------------------------------

# 19. Docker Logs

Stream processor:

``` powershell
docker compose -f docker-compose.deploy.yml logs -f stream_processor
```

Persistent storage:

``` powershell
docker compose -f docker-compose.deploy.yml logs -f stream_storage
```

Analytics:

``` powershell
docker compose -f docker-compose.deploy.yml logs -f analytics
```

Kafka:

``` powershell
docker compose -f docker-compose.deploy.yml logs -f kafka
```

View all services:

``` powershell
docker compose -f docker-compose.deploy.yml logs -f
```

------------------------------------------------------------------------

# 20. Stopping the Deployment

Stop containers:

``` powershell
docker compose -f docker-compose.deploy.yml down
```

Stop containers and remove named volumes:

``` powershell
docker compose -f docker-compose.deploy.yml down -v
```

> `down -v` removes the Kafka and SQLite persistent volumes. Use it only
> when you intentionally want to reset the deployment state.

------------------------------------------------------------------------

# 21. Verification Checklist

## Research core

-   [ ] TATKAL-X simulation starts
-   [ ] Baseline/self-tests pass
-   [ ] Controller modes operate correctly
-   [ ] Research outputs are generated

## Event layer

-   [ ] Structured event generated
-   [ ] Event contains expected fields
-   [ ] Simulation ID is preserved
-   [ ] Controller mode is preserved

## Kafka

-   [ ] Kafka broker is running
-   [ ] `tatkal-x-events` topic is available
-   [ ] Events are published
-   [ ] Kafka consumer receives live events

## Stream processing

-   [ ] Rolling state metrics update
-   [ ] Delta rates update correctly
-   [ ] Retry amplification is calculated
-   [ ] Anomaly flags update

## Persistent storage

-   [ ] SQLite database is created
-   [ ] Events are stored
-   [ ] Simulation IDs can be queried
-   [ ] Controller modes can be queried

## Interactive analytics

-   [ ] Analytics API starts
-   [ ] `/health` responds
-   [ ] Simulation selector works
-   [ ] Metric selector works
-   [ ] Charts update
-   [ ] Event explorer works

## Docker

-   [ ] Docker image builds
-   [ ] Kafka container is healthy
-   [ ] Stream processor starts
-   [ ] Storage starts
-   [ ] Analytics starts
-   [ ] Port 5001 is accessible

------------------------------------------------------------------------

# 22. End-to-End Data Flow

A complete live run follows this sequence:

``` text
1. TATKAL-X Engine.tick()
        |
        v
2. Research state is updated
        |
        v
3. Event is constructed
        |
        v
4. Kafka producer publishes JSON event
        |
        v
5. Kafka topic: tatkal-x-events
        |
        +--------------------------+
        |                          |
        v                          v
6A. Stream Processor       6B. Persistent Storage
        |                          |
        v                          v
7A. Online metrics         7B. SQLite database
        |                          |
        |                          v
        |                   Analytics API
        |                          |
        +------------+-------------+
                     |
                     v
              Interactive UI
```

------------------------------------------------------------------------

# 23. Reproducibility and Research Integrity

The infrastructure layer is deliberately separated from the research
core.

The following principle should be maintained:

``` text
Research model
      !=
Streaming infrastructure
      !=
Visualization layer
```

Kafka should transport events rather than alter them.

The stream processor should observe events rather than control the
simulation.

The SQLite layer should persist events rather than modify the simulation
state.

The analytics UI should query stored events rather than introduce new
research assumptions.

This separation makes it easier to:

-   reproduce experiments
-   benchmark the research model
-   inspect event histories
-   compare controller modes
-   debug runtime behavior
-   deploy the supporting infrastructure
-   distinguish research contributions from engineering components

------------------------------------------------------------------------

# 24. Research Contribution vs Engineering Support

  Component                             Role
  ------------------------------------- ---------------------------
  Finite-horizon reservation dynamics   Core research
  Flash-crowd synchronization           Core research
  Endogenous retry pressure             Core research
  Queue/service dynamics                Core research
  Inventory/payment dynamics            Core research
  MPC controller                        Core research
  CBF safety filter                     Core research
  Event schema                          Supporting infrastructure
  Kafka                                 Supporting infrastructure
  Stream processor                      Supporting analytics
  SQLite                                Supporting persistence
  Flask analytics UI                    Supporting observability
  Docker Compose                        Deployment infrastructure

The novelty of TATKAL-X should therefore be evaluated primarily through
the reservation-dynamics framework, control formulation, experimental
methodology, and results---not through the use of Kafka or Docker alone.

------------------------------------------------------------------------

# 25. Troubleshooting

## Kafka connection refused

Check:

``` powershell
docker compose -f docker-compose.deploy.yml ps
```

Then inspect Kafka:

``` powershell
docker compose -f docker-compose.deploy.yml logs kafka
```

For a host-side application, use:

``` text
localhost:29092
```

For Docker services, use:

``` text
kafka:9092
```

Do not interchange these addresses.

------------------------------------------------------------------------

## Analytics database unavailable

Check storage:

``` powershell
docker compose -f docker-compose.deploy.yml logs stream_storage
```

The storage service must be able to connect to:

``` text
kafka:9092
```

The analytics service uses:

``` text
/app/data/tatkal_x_stream.db
```

through the shared `tatkal_data` Docker volume.

------------------------------------------------------------------------

## Stream processor receives no events

Check:

``` powershell
docker compose -f docker-compose.deploy.yml logs -f stream_processor
```

Then verify that the simulation is actually publishing events to:

``` text
tatkal-x-events
```

Also verify that the producer and consumer use compatible bootstrap
addresses.

------------------------------------------------------------------------

## Docker build fails

First run:

``` powershell
docker version
```

and:

``` powershell
docker compose version
```

Then rebuild:

``` powershell
docker compose -f docker-compose.deploy.yml build --no-cache
```

If the build still fails, use the exact Docker error output to diagnose
the failing layer rather than changing the research code.

------------------------------------------------------------------------

# 26. Recommended Startup Order

For a host-side simulation with Docker infrastructure:

``` text
1. Start Docker
2. Start docker-compose.deploy.yml
3. Verify Kafka
4. Verify stream processor
5. Verify stream storage
6. Open analytics UI
7. Start TATKAL-X simulation with Kafka enabled
8. Observe live Kafka events
9. Verify stream metrics
10. Verify SQLite persistence
11. Inspect interactive analytics
```

For research-only experiments where streaming is not required:

``` text
1. Run the research core
2. Run self-tests
3. Generate experimental results
```

This keeps infrastructure from becoming a requirement for reproducible
research computation.

------------------------------------------------------------------------

# 27. License / Citation

If this project is used in an academic publication, cite the TATKAL-X
research work according to the final publication metadata.

Infrastructure technologies used by the implementation include:

-   Apache Kafka
-   Python
-   Flask
-   SQLite
-   Docker
-   Docker Compose

These technologies are implementation dependencies and are not, by
themselves, the claimed research contribution.

------------------------------------------------------------------------

## TATKAL-X

**Finite-Horizon Reservation Dynamics for Synchronized Flash-Crowd
Reservation Systems with Endogenous Behavioral Feedback and Safety-Aware
Predictive Control**
