# Project Context

We are building a production-grade **Real-Time Electronic Trading Platform** in Java.

The project is intended to demonstrate engineering skills relevant to software, backend, platform, trading-infrastructure and real-time systems roles at hedge funds, quantitative trading firms, market makers and financial technology companies.

The system should remain broadly relevant to firms such as Millennium, IMC, Tower Research, Squarepoint, Qube Research & Technologies, WorldQuant, AlphaGrep, Virtu, Optiver, D. E. Shaw, Point72 and similar organizations.

Do not design this as a tutorial, CRUD stock application or generic microservices demo.

The engineering value of the project comes from:

* deterministic processing
* concurrency
* real-time event processing
* low-latency design
* order and execution lifecycle
* market data
* Kafka/event-driven architecture
* reliability and recovery
* idempotency
* ordering guarantees
* backpressure
* JVM performance
* networking
* persistence
* observability
* profiling
* benchmarking
* production operations

---

# Primary Technology

Use:

* Java 21
* Maven
* Spring Boot where appropriate
* Apache Kafka
* PostgreSQL
* Docker
* Docker Compose
* Testcontainers
* JUnit
* Micrometer
* Prometheus
* Grafana

Do not introduce another programming language for core backend functionality unless there is a strong engineering reason.

Latency-sensitive domain components such as the matching engine should remain independent from Spring where practical.

---

# Architecture Philosophy

Prefer a modular monorepo initially.

Do not create unnecessary microservices.

Separate:

1. latency-sensitive processing;
2. asynchronous event processing;
3. persistence;
4. control-plane/API operations;
5. observability.

The matching engine must not depend directly on databases, Kafka, HTTP or Spring.

PostgreSQL and Redis must not automatically be placed in critical execution paths.

Every external dependency must have a clear engineering reason.

Prefer simple, measurable designs before complex optimizations.

Never introduce complexity merely because it appears impressive.

---

# Project Portability

The project must be extremely easy to run on:

* a developer laptop;
* another developer's machine;
* CI;
* a VM;
* Docker;
* Kubernetes;
* AWS;
* Azure;
* GCP.

Avoid machine-specific assumptions.

The application must not depend on manually installed infrastructure except Docker and a supported JDK when running outside containers.

Whenever possible, the full development environment should start with:

```bash
docker compose up --build
```

or through a single documented helper command such as:

```bash
make up
```

Moving between local development and cloud environments should primarily require changing configuration, not rewriting application code.

---

# Containerization Rules

Every deployable application must have a Dockerfile.

Prefer multi-stage Docker builds.

Docker images should:

* be reproducible;
* contain required runtime dependencies;
* avoid unnecessary development tools;
* run as a non-root user where practical;
* expose documented health checks;
* support configuration entirely through environment variables;
* avoid embedding secrets;
* have reasonable image size.

Docker Compose should provision all local dependencies required for development, such as:

* Kafka
* PostgreSQL
* Prometheus
* Grafana

Add services only when they are actually required.

A new developer should not need to manually install Kafka or PostgreSQL.

---

# Configuration Rules

Application configuration must follow environment-based configuration.

Never hardcode:

* passwords
* API keys
* tokens
* database credentials
* Kafka credentials
* cloud credentials
* host-specific URLs
* encryption keys

Use environment variables.

For local development, provide:

```text
.env.example
```

with safe placeholder values.

The real:

```text
.env
```

must be excluded through `.gitignore`.

Example:

```text
DB_HOST=postgres
DB_PORT=5432
DB_NAME=trading
DB_USERNAME=trading
DB_PASSWORD=change-me

KAFKA_BOOTSTRAP_SERVERS=kafka:9092

TRADING_GATEWAY_PORT=8080

RISK_MAX_ORDER_QUANTITY=10000
RISK_MAX_ORDER_NOTIONAL=1000000
```

Do not commit real credentials.

Spring configuration should reference variables using patterns such as:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
```

Provide sensible non-secret defaults only where doing so is safe.

---

# Cloud Readiness

Infrastructure providers must not leak deeply into domain code.

Prefer abstractions and standard protocols.

Examples:

* PostgreSQL instead of cloud-specific database APIs;
* Kafka protocol rather than cloud-specific messaging APIs where practical;
* environment-variable configuration;
* standard container images;
* OpenTelemetry/Micrometer-compatible observability.

Deployment should remain portable.

A future migration should look approximately like:

```text
Local
Docker Compose
      ↓
Container Registry
      ↓
VM / Kubernetes / ECS / AKS / GKE
```

Application code should require little or no modification.

Do not introduce Kubernetes manifests until the project actually needs deployment orchestration.

When Kubernetes is introduced later, keep manifests under:

```text
deploy/kubernetes/
```

Cloud-specific configuration may live under:

```text
deploy/aws/
deploy/azure/
deploy/gcp/
```

but domain/application code should remain cloud-neutral.

---

# Secrets

Local development:

```text
.env
```

Production:

use the target platform's secret-management system.

Examples include:

* Kubernetes Secrets / external secret managers
* AWS Secrets Manager
* Azure Key Vault
* Google Secret Manager

Applications should consume secrets through environment variables or mounted secrets without knowing which provider supplied them.

Never place production credentials in:

* source code;
* application.yml;
* Dockerfile;
* Docker Compose committed values;
* README files;
* tests;
* Git history.

---

# Dependency Management

All Java dependencies must be declared through Maven.

Do not require developers to manually download libraries.

Pin important dependency/plugin versions where reproducibility matters.

Avoid unnecessary dependencies.

Before adding a library, ask:

1. What problem does it solve?
2. Can the JDK already solve it cleanly?
3. Does it enter the hot path?
4. What operational/runtime cost does it introduce?

---

# Repository Structure

Target structure:

```text
trading-platform/
├── AGENTS.md
├── README.md
├── pom.xml
├── .env.example
├── .gitignore
├── docker-compose.yml
│
├── trading-domain/
├── trading-gateway/
├── matching-engine/
├── market-data/
├── execution-pipeline/
├── risk-engine/
├── portfolio-service/
├── persistence/
├── exchange-simulator/
├── benchmark/
├── observability/
├── integration-tests/
│
├── docker/
├── deploy/
└── docs/
```

Do not create empty modules solely to satisfy this structure. Add modules as their corresponding stages are implemented.

---

# Code Quality

Code should be production-oriented.

Prefer:

* small cohesive classes;
* explicit domain models;
* immutable events;
* Java records where appropriate;
* constructor injection;
* meaningful exception types;
* explicit error handling;
* strong tests;
* descriptive names.

Avoid:

* giant service classes;
* unnecessary interfaces;
* pointless factories/builders;
* fake abstractions;
* excessive comments;
* generated-looking comments;
* silent exception swallowing;
* global mutable state;
* premature optimization.

Comments should explain **why**, not restate what the code does.

---

# Trading-System Correctness

Correctness is more important than benchmark numbers.

Important invariants include:

```text
executed quantity <= original quantity
remaining quantity >= 0
filled orders cannot execute again
cancelled orders cannot execute
an execution affects portfolio state exactly once
price-time priority must always hold
replaying the same ordered input must recreate the same state
```

Any change touching these invariants must include tests.

---

# Performance Rules

Never fabricate performance results.

Never optimize solely based on intuition.

Follow:

```text
baseline
   ↓
measure
   ↓
profile
   ↓
identify bottleneck
   ↓
change
   ↓
measure again
   ↓
document trade-off
```

Use appropriate tools later:

* JMH
* Java Flight Recorder
* async-profiler
* jcmd
* jstat
* Prometheus
* Grafana

Relevant measurements include:

* throughput;
* p50;
* p95;
* p99;
* p99.9;
* CPU;
* memory;
* allocation rate;
* GC pauses;
* queue depth;
* Kafka consumer lag.

Store real benchmark evidence under:

```text
benchmarks/results/
```

---

# Reliability Rules

Assume that components fail.

Design consciously for:

* retries;
* duplicate messages;
* out-of-order events;
* process restart;
* Kafka outages;
* database outages;
* queue saturation;
* slow consumers;
* network interruptions;
* traffic spikes.

Do not add retries blindly.

Every retry mechanism must consider:

* idempotency;
* maximum attempts;
* timeout;
* backoff;
* failure destination;
* observability.

---

# Observability

All deployable services should eventually expose:

```text
/actuator/health
/actuator/prometheus
```

where appropriate.

Use structured logs.

Every order/execution flow should be traceable using identifiers such as:

```text
correlationId
orderId
clientOrderId
executionId
accountId
symbol
```

Do not log secrets.

---

# Database Management

PostgreSQL schema changes must use Flyway.

Never require developers to manually create tables.

Starting the environment should automatically create/migrate the required schema.

Database migrations belong in version control.

Indexes must have documented reasons.

---

# Testing

Each stage must maintain or improve tests.

Use the appropriate combination of:

* unit tests;
* integration tests;
* Testcontainers;
* concurrency tests;
* replay tests;
* failure tests;
* performance tests.

Do not mark a stage complete if existing tests are broken.

---

# Documentation

Important architecture decisions belong under:

```text
docs/architecture-decisions/
```

Use ADRs for decisions such as:

```text
single-writer order books
Kafka partitioning
idempotency
recovery model
serialization
backpressure
persistence model
```

Keep documentation synchronized with implementation.

Do not document functionality that does not exist.

---

# Developer Experience

The repository should eventually support a workflow approximately like:

```bash
git clone <repo>

cp .env.example .env

docker compose up --build
```

Then:

```bash
curl http://localhost:8080/actuator/health
```

should demonstrate that the environment is operational.

Prefer one-command workflows.

If additional manual setup is unavoidable, document exactly why.

---

# Codex Working Rules

Before changing code:

1. inspect the current repository;
2. understand relevant existing modules;
3. read applicable ADRs/docs;
4. preserve existing architecture unless the task explicitly changes it.

After changing code:

1. compile the affected modules;
2. run relevant tests;
3. run formatting/static checks if configured;
4. update documentation if architecture changed;
5. provide exact commands used;
6. report any failing tests or unresolved issues.

Never claim something works without running the relevant validation when execution is available.

Do not implement future project stages unless explicitly requested.

When uncertain about an architectural decision, prefer the simplest correct implementation that leaves room for measurement and later evolution.

---

# Core Principle

This repository should demonstrate:

> **A Java engineer capable of reasoning about production-grade, real-time, performance-sensitive distributed systems.**

It should not attempt to pretend to be the proprietary trading infrastructure of a real financial firm.

Engineering credibility comes from correctness, architectural reasoning, failure handling, measurements and explainable trade-offs.
