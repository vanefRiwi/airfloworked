# Apache Airflow — Architecture, Execution Flow & Executors

A practical walkthrough of what Airflow is (and what it is not), the components that make it work, how a DAG travels through the system from file to finished run, and the executors you can choose between.

---

## Table of Contents

- [What Airflow Is Not](#what-airflow-is-not)
- [What Airflow Actually Does](#what-airflow-actually-does)
- [Building Blocks: The DAG](#building-blocks-the-dag)
- [The DAG Lifecycle](#the-dag-lifecycle)
- [Airflow Architecture](#airflow-architecture)
  - [1. Metadata Database](#1-metadata-database--mandatory)
  - [2. Scheduler](#2-scheduler--mandatory)
  - [3. Executor](#3-executor--mandatory)
  - [4. Web Server / API Server](#4-web-server--api-server--optional-but-best-practice)
  - [5. Workers](#5-workers--depends-on-the-executor)
  - [6. DAG Processor](#6-dag-processor--optional-recent-addition)
  - [7. Message Broker](#7-message-broker--celery-only)
- [Execution Flow](#execution-flow)
- [Component Responsibilities at a Glance](#component-responsibilities-at-a-glance)
- [Executors](#executors)
  - [Local Executor](#local-executor)
  - [Sequential Executor](#sequential-executor)
  - [Celery Executor](#celery-executor)
  - [Kubernetes Executor](#kubernetes-executor)
- [Executor Comparison](#executor-comparison)
- [Installation Note](#installation-note)

---

## What Airflow Is Not

**Airflow is not an ETL tool.** It does not extract, transform, or load your data.

What it does is automate the workflow. It is not processing — it is an orchestrator. We only automate the flow: *when* your Python code runs, and *how* it runs.

A few more things Airflow is not:

- **It is scheduled, not real time.** It has scheduling, but that scheduling is never real time, so don't reach for Airflow when you need real-time processing.
- **It is not worth it for a single task.** Airflow is a heavy structure — you have to build a lot of architecture around it before anything runs at all.

---

## What Airflow Actually Does

Airflow only triggers the process, whether that process is Python, SQL, or PySpark.

You define a task, you tell Airflow when it should happen, and Airflow triggers it at that particular time. Nothing else.

---

## Building Blocks: The DAG

**The DAG defines what should run and in what order.**

An Airflow installation has several folders and volumes, including the one for logs. The one that matters here is the `dags` folder: you write your DAG code, save the file, and that file lands in the DAGs folder. Only then can Airflow see it.

Once the file is there, Airflow scans that folder frequently — the scan is done by the scheduler — and picks the DAG file up. The file is a plain Python file, let's say `abc.py`.

---

## The DAG Lifecycle

### 1. Parse

Before Airflow executes anything, it parses the file. This happens before the DAG ever appears in the user interface.

If there is a broken task, broken logic, or a syntax error, Airflow raises an error — and sometimes it won't even tell you clearly what happened. At this stage nothing runs; the file is only being read. If the flow is broken, you have to fix it before going any further.

Once it parses cleanly, Airflow reads the blueprint of the DAG. Parsing here means segregating all the tasks the DAG contains and the relationships between them.

### 2. Schedule

The parsed DAG then goes to the scheduler, which decides **when** the DAG run has to be executed. That decision is based on the schedule interval or on dependencies. When the schedule interval triggers, or the criteria are met, the DAG run is launched.

### 3. Execute

The run is handed off for execution.

> **In short:** first it gets parsed, then scheduled, then executed. That's the DAG lifecycle.

---

## Airflow Architecture

Airflow is not a single program you install like a normal application. It is a **combination of components**, and those components together are what make Airflow work.

Some of these components are **mandatory** — Airflow cannot run without them. The rest are **optional**, and you add them according to your requirements.

There are several ways to install Airflow, but these notes use Docker containers.

### 1. Metadata Database — *mandatory*

The metadata database is the backbone of Airflow. It is a personal database that Airflow manages for itself, and it holds all the state: DAG state, task state, run history, schedules, retries, and logs.

Retries are a good example of what lives here. The number of times a task retries is defined in the DAG itself, and every one of those attempts — along with the task runs, the DAG state, and the task state — is recorded in the metadata database.

### 2. Scheduler — *mandatory*

**The scheduler is the brain.**

Whenever a DAG lands in the DAGs folder, the scheduler parses the file and schedules it accordingly. It also writes the task runs into the metadata database and decides when the DAG should run.

That decision is based on the schedule interval or the dependencies: you define when the DAG runs should happen, and when the interval triggers or the criteria are met, the DAG is executed.

Without the scheduler, Airflow cannot run.

### 3. Executor — *mandatory*

**The executor decides *how* the tasks are run.**

The division of labour is clean here. The scheduler parses and decides *when* the DAG runs should happen; the executor takes over and decides *how* the tasks inside that DAG should run.

There are four types of executors — Local, Celery, Kubernetes, and Sequential — and all four behave differently. We'll go into each of them [later on](#executors).

One thing to be clear about: the executor is not business logic. It doesn't run any logic of its own. It only decides how the tasks should be run.

### 4. Web Server / API Server — *optional, but best practice*

The web server is the user interface. Its job is visualization: it reads state from the metadata database and shows it to you.

It performs no core execution and does not participate in the DAG runs at all. It simply displays what the metadata database already contains — DAG runs, logs, retries, task states.

The web server is also referred to as the **API Server**.

> **Summary so far:** the metadata database, the scheduler, and the executor are the three mandatory components. The web server is optional, though including it is best practice.

### 5. Workers — *depends on the executor*

Everything up to this point has been about coordination and relationships between components. Workers are different: they are **pure task execution**.

Whether you need them depends on your executor. They are required for the Celery Executor; for the rest, they are optional.

Workers are what let Airflow handle heavy workloads without breaking a sweat. For personal use the difference is minor, but in an industrial setting the load on the system is enormous — you cannot process that volume of data on a single PC. The work has to be split across separate machines, and those separate machines are your workers. You distribute the load across them and run your processes there.

**One clarification worth making:** distributing the load is not exclusive to Celery. Even without workers, you can run the scheduler on one machine, the executor on another, and the metadata database on a third. You are free to spread the components across multiple machines to distribute the load — it does not have to be a worker–executor relationship.

### 6. DAG Processor — *optional, recent addition*

The DAG Processor is a newer component. It did not exist in the legacy versions of Airflow and was added relatively recently.

Its job is to share the parsing load with the scheduler. It pulls the incoming DAG file from the DAGs folder, parses it, and separates the tasks according to the relationships defined between them. That's the whole purpose of the component.

Before the DAG Processor existed, the scheduler did all of this alone: taking the file from the DAGs folder, parsing it, scheduling it, and writing the DAG run logs into the metadata database. The DAG Processor was introduced to split that load more evenly across machines.

It is optional. You don't have to define it. The three components you actually need are the **scheduler, executor, and metadata database** — everything else is optional.

### 7. Message Broker — *Celery only*

With the Celery Executor you use multiple workers, and those workers run on different machines. Because of that, the scheduler cannot hand tasks to them directly.

To transfer the tasks and communicate with the workers, we use a **message broker**.

The message broker sits between the executor and the workers as an intermediate component, and its role is to deliver each task to the worker that needs to run it.

The most commonly used message brokers are **Redis** and **RabbitMQ**.

---

## Execution Flow

**1.** You write the DAG file with its tasks, and the scheduler picks the file up.

**2.** The file is parsed. If there is no DAG Processor, the scheduler does the parsing itself, because the scheduler is the brain. If a DAG Processor is active, it takes the file from the DAGs folder and parses it first.

**3.** Airflow extracts the blueprint of the DAG and saves it in the metadata database. Note that the DAG itself is not stored there — only its blueprint.

**4.** The schedule for when the DAG runs should happen is also written to the metadata database, and that part is handled by the scheduler. The scheduler writes everything it knows about the DAG runs into the database.

> At this point the DAG has been **parsed** and **scheduled**.

**5.** The scheduler asks the executor to run the task.

**6.** The executor splits the tasks accordingly, depending on which worker is actually available to run them. With Celery it distributes the work; with Local it simply executes on the same machine.

**7.** The tasks go out to the workers, if there are workers. With Local or Kubernetes, execution follows that executor's own model instead of using Celery workers.

**8.** The logs generated by the DAG runs are written back to the metadata database by the worker. Failed runs and successful runs alike — all the logs and task states go back into the database.

**9.** Finally, the web server reads from the metadata database and shows you the updates in the UI.

That's how the flow goes.

```
DAG file
  → DAG Processor or Scheduler parses the DAG
  → DAG blueprint and run information are stored in the Metadata Database
  → Scheduler decides when to run
  → Executor launches the tasks
  → Message Broker delivers tasks when using Celery Executor
  → Workers run the tasks
  → Task state and logs are written to the Metadata Database
  → Web Server reads the Metadata Database and shows the information in the UI
```

---

## Component Responsibilities at a Glance

| Component | Role | What it does |
| --- | --- | --- |
| **Scheduler** | Decision maker | Decides *when* to run. It does not run the task. |
| **Executor** | Launcher | Launches the tasks without running them, splitting them between the workers that are available. |
| **Worker** | Runner | Actually runs the task. |
| **Metadata Database** | Memory | Airflow's own database — logs, task state, DAG state, run history, schedules, retries. |
| **Web Server / API Server** | Visibility | A dashboard showing what's happening. Takes no part in execution. |

The easiest way to hold this in your head: **the executor is a launcher, the worker is a runner.** The scheduler only decides when things should happen, and the database is simply the memory.

Airflow will execute perfectly well without a web server — you just won't be able to see what's happening. The web server exists to make monitoring your DAG runs easy.

---

## Executors

The available executors are the **Local Executor**, the **Celery Executor**, the **Kubernetes Executor**, and the **Sequential Executor**. Sequential is so close to Local that a single detail separates them.

**Choosing an executor never changes your tasks or their logic. It only changes how those tasks are executed.** That's it.

What an executor gives you is the ability to scale horizontally: add more workers, and they work in parallel on the same DAG.

### Local Executor

This is the simplest executor of the four.

The scheduler sits at the top, and the Local Executor sits beneath it. There are no additional workers here — there is just one worker, and that worker is the machine itself. The scheduler, the executor, and the worker all live on the same machine, and nothing is split anywhere. Everything is native to that one machine.

That simplicity is exactly why it is **not production grade**. You will not see the Local Executor in real companies. Because everything runs on a single machine, the work is never divided.

Where it shines is learning and local testing. The setup is trivial, the infrastructure is easy to stand up, and you can get running quickly. Good for learning, testing, and development — not for production.

### Sequential Executor

The one difference: in the Local Executor **all tasks run in parallel**, while in the Sequential Executor **only one task runs at a time**. The next task waits until the currently running task has finished successfully.

Everything else about its architecture is identical to the Local Executor.

### Celery Executor

The Celery Executor works quite differently.

As with any other setup, everything starts at the scheduler, which produces the DAG plan — the blueprint. Below the scheduler sits the Celery Executor, and below the executor sits the **message broker**.

The tasks travel from the executor to the workers through that broker. And these are no longer a single worker: you now have **multiple distributed workers**, any number of them, configured according to your needs and your load. The work is distributed across whichever workers are available, and the broker is what delivers each task to its worker.

The Celery Executor is the **classic production-grade executor**. It is stable and horizontally scalable.

That scalability is the key contrast with the Local Executor. Local cannot scale horizontally at all. Celery can, because you can use multiple workers for the same DAG file and assign different tasks within that DAG to different workers.

### Kubernetes Executor

Unlike Celery, the Kubernetes Executor **eliminates the message broker entirely**. It reaches the workers directly.

The Celery Executor is production grade and handles constant workloads well. What makes Kubernetes different is that it is **modern and cloud-native**. There are no VMs and no physical machines to manage — everything is cloud-native, and that is also what makes it **auto-scalable**.

Kubernetes starts and closes pods according to the load. Three tasks means three pods. **Pods are simply workers** — the name changes in Kubernetes, but they are the same thing as Celery's workers.

Instead of keeping a fixed number of workers running constantly, pods are created and eliminated as needed. That directly **reduces billing cost**: because Kubernetes is cloud-native, your cloud provider charges you based on how long you run and how many pods you have. Auto-scaling means only the pods your actual load requires are ever started, and the rest are eliminated.

In theory Kubernetes is more powerful than Celery — and in practice it usually is — but it comes with real cost. It is operationally complex and harder to set up and manage than Celery. It is still the more modern option, and better than Celery in a lot of ways.

A concrete example of the difference: imagine a DAG with many tasks running on Celery. You assign those tasks to your workers, but once you exceed the workers' load limit, the remaining tasks **wait in the queue** until a worker frees up. In Kubernetes, nothing waits — a new pod is created for that specific task and eliminated once the task is done.

That is the standout feature of the Kubernetes Executor.

---

## Executor Comparison

| | **Local Executor** | **Celery Executor** | **Kubernetes Executor** |
| --- | --- | --- | --- |
| **How it runs** | Single machine | Distributed workers | One pod per task |
| **Message broker** | Not needed | Required (Redis, RabbitMQ) | Not needed |
| **Scalability** | Low | Medium to high | Very high (auto-scaling) |
| **Setup complexity** | Very low | Medium | High |
| **Typical use** | Learning, testing, development | Classic production model | Modern, cloud-native production |

**Scalability.** The Local Executor scores low because there is only one worker — one machine for every component, with nothing split across machines. Celery lands between medium and high: it scales, but only up to the specific number of workers you have provisioned. Kubernetes is always ready for heavy loads, including unexpected ones, which makes it the best fit for that kind of situation.

**Saving computational power.** This is where Kubernetes really pulls ahead. Suppose you keep 100 workers running every day. Even if your actual data load only needs 30, you keep all 100 alive, because you never know what's coming.

With Kubernetes, you don't control the worker count at all — Kubernetes handles it. When the load increases the number of pods increases; when it decreases the extra pods are eliminated automatically. Excellent for scalability, at the price of added complexity.

**Complexity.** The Local Executor is very low complexity and trivial to set up. Celery is more involved, since you also have to set up workers on separate machines — virtual or physical. Kubernetes has the highest setup complexity of the three, but it is worth it.

---

## Installation Note

For learning purposes we'll use the **Local Executor**, installed with **Docker**.

There are plenty of ways to install Airflow directly on your PC, and it isn't difficult. I'm choosing the Docker image instead because it stays much cleaner: once you're done learning, cleaning up the files Airflow leaves behind on a local installation is a genuinely tedious job.

Docker is far easier to remove than a local installation, so that's the route these notes take.
