# Apache Airflow — Architecture, Execution Flow & Executors

Notes on what Airflow is (and what it is not), the components that make it work, how a DAG travels through the system, and the different executors you can run it with.

---

## Table of Contents

- [What Airflow Is Not](#what-airflow-is-not)
- [What Airflow Actually Does](#what-airflow-actually-does)
- [Building Blocks: The DAG](#building-blocks-the-dag)
- [The DAG Lifecycle](#the-dag-lifecycle)
- [Airflow Architecture](#airflow-architecture)
  - [Metadata Database](#1-metadata-database-mandatory)
  - [Scheduler](#2-scheduler-mandatory)
  - [Executor](#3-executor-mandatory)
  - [Web Server / API Server](#4-web-server--api-server-optional-but-best-practice)
  - [Workers](#5-workers-depends-on-the-executor)
  - [DAG Processor](#6-dag-processor-optional-recent-addition)
  - [Message Broker](#7-message-broker-celery-only)
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

**Airflow is not an ETL tool.** It does not help you extract, or transform, or load data.

It just automates the workflow. It is not processing — it's just an orchestrator. We only automate the flow: *when* to run Python code and *how* to run it.

A few more things Airflow is not:

- **It's scheduled, not real time.** Even though it has scheduling, it's never real time. Don't use it for real-time work.
- **Don't use it if you are going to execute only one task.** It's a heavy structure, since you have to build a lot of architecture around it.

---

## What Airflow Actually Does

It only triggers the process — Python, SQL, PySpark.

What you do is: you have a task, and Airflow triggers it at the particular time you indicate. Airflow does that for you. Nothing else.

---

## Building Blocks: The DAG

**DAG** — defines *what* should run and *in what order*.

When you're working with Airflow, there are multiple other folders and volumes, which include the logs. There is this one called `dags`, where you place your DAG files. You write your DAG code, save the file, and this file will be saved in that DAGs folder. Only then can Airflow scan that DAG file.

Once the file is in the DAGs folder, Airflow scans that folder often — it's scanned by the scheduler, and it picks that DAG file up. The DAG file will be, let's say, `abc.py`, so it's a Python file.

---

## The DAG Lifecycle

Airflow scans the DAGs folder often. When it finds the file, it picks it up.

**1. Parse.** Before it executes the DAG by itself, it won't even show you the DAG in the user interface. It first parses it. If there is any broken task, broken logic, or syntax errors, it will pop an error — or sometimes it won't even show you what happened. At that stage, it just parses the DAG file. If there is a broken flow, it shows you, and you have to fix it.

After you fix it, it takes that DAG file and reads the blueprint. Parsing actually means segregating all the tasks the DAG has.

**2. Schedule.** This parsed task will then go to the scheduler. So the DAG will first get parsed, and then it goes to the scheduler. What happens in the scheduler is that the scheduler decides *when* the DAG run has to be executed. Based on the schedule interval or the dependency, you schedule the DAG runs — and when the schedule interval triggers or meets the criteria, then the DAG will get executed.

**3. Execute.**

> **In short:** first it gets parsed, then scheduled, then executed. That's it. That's the DAG lifecycle.

---

## Airflow Architecture

Airflow is not just a single program that you can install like an application. It is a **combination of components** that you need in order to run Airflow.

There are a lot of ways to install it on your PC, but in these notes we install Airflow using Docker containers. When we install Airflow, we have to install the components — and among these components there are a few that are **mandatory** and required to run Airflow, and a few that are **optional**, which you use according to the requirement.

These components combined make our Airflow work.

### 1. Metadata Database *(mandatory)*

That's the backbone. It stores all the state of Airflow — task status, DAG runs, everything.

Just like the name says, it's a database — like a personal database that Airflow manages — where it stores all the logs and all the data that it has: the DAG state, the task state, the run history, the schedules, and the retries.

The amount of times it retries will be defined in the DAG: the DAG will define the amount of retries a task will go through. So there are the retries, and the task logs — I mean, everything: the task runs, DAG state, task state, everything will be stored in the metadata database.

### 2. Scheduler *(mandatory)*

**Scheduler is like a brain.** Whenever a DAG gets into the DAGs folder, the scheduler will parse the DAG file and schedule it accordingly. And like I said, it writes the task runs to the metadata database and decides when the DAG should be run.

Basically, the scheduler decides when the DAG runs should be executed. Based on the schedule interval or the dependency, you schedule the DAG runs, and when the schedule interval triggers or meets the criteria, then the DAG will get executed.

Without the scheduler, Airflow cannot run.

### 3. Executor *(mandatory)*

**The executor decides *how* these tasks are run.**

The scheduler parsed it and decided *when* these DAG runs should be scheduled. Now the executor decides *how* these tasks in the DAG should run.

There are a lot of types of executors, which include Local Executor, Celery Executor, Kubernetes Executor, and there's also Sequential Executor. There are four types of executors, and all four are different — we'll dive deep into those [later](#executors).

So the executor is not business logic. It does not run any logic, it just decides how these tasks should be run.

### 4. Web Server / API Server *(optional, but best practice)*

Basically a web server is just a user interface, a UI, just to see, just to visualize — by reading from the metadata database. It just reads that state, so it doesn't do any core execution. It doesn't participate in any core part of the DAG runs.

It just shows you what's in the metadata database: the logs, the DAG runs, the retries. But it does not contribute to the DAG execution — it just shows you the logs.

One more thing about this: the Web Server can also be called the **API Server**.

> So the metadata database, the scheduler, and the executor are the three main components needed to run Airflow. The Web Server is not mandatory, it's just optional — but it's a best practice.

### 5. Workers *(depends on the executor)*

Until now, all these components are about relation and coordination. But these components — the workers — are **pure task execution**.

Depending on the executor, these components are required. They are only required for the Celery Executor; for the rest of the executors we don't need them, so they're optional.

These workers let Airflow run flawlessly with heavy loads of work. For our personal use it is not so different, but for industrial use there is a lot of load on the system. We cannot hold that load — we cannot process a humongous amount of data with that kind of load on just one PC. We have to split that work separately across different machines.

For that, we define those separate machines as **workers**, and then we use those machines for the processes. So we just distribute the load across the machines using workers.

**An important clarification:** when I say we distribute the load in Celery Executor alone, I don't mean we only use multiple machines in Celery Executor. Even with the regular executors without workers, we can run the scheduler on one machine, the executor on another machine, the metadata database on another machine — so we can just split these components.

We can spread these components across multiple machines in order to distribute the load. It does not necessarily have to be a worker–executor relation to distribute this workload.

### 6. DAG Processor *(optional, recent addition)*

This is another component that was recently added into Airflow. It wasn't in the legacy Airflow, in the old Airflow versions, but recently they also added a component called the **DAG Processor**.

The DAG Processor splits the load with the scheduler. The responsibility of this component is to parse the incoming DAG file and separate these tasks according to their defined relation between the tasks. So it just parses the DAG file. The whole purpose of the processor is to pull the DAG file from the DAGs folder and parse it.

Before this, before the DAG Processor was in the picture, the scheduler used to do the whole process: taking the DAG file from the DAGs folder and processing it — which includes parsing the DAG file, the scheduling, and writing the logs of the DAG runs into the metadata database.

Everything used to be done using the scheduler alone, but they introduced the DAG Processor to split the load evenly across the machines.

That is relatively new, and it is also optional. You don't have to define this component. You just need these three components — **scheduler, executor, metadata database** — and the rest of the components are optional.

### 7. Message Broker *(Celery only)*

When you use Celery Executor, you use multiple workers. The tasks that are given to the workers from the scheduler cannot be directly given to the workers, because they run on a different PC.

To actually transfer these tasks and communicate with the worker, we use something called a **message broker**.

So we use a message broker as an intermediate component in between the executor and the worker, to transfer the task, to deliver the task to the worker.

The most commonly used message brokers include **Redis** and **RabbitMQ**. We use those brokers to deliver the tasks to the workers.

---

## Execution Flow

Now let's talk about the flow.

**1.** We write the DAG file with the tasks. Then the scheduler will pick up the file.

**2.** If there is no DAG Processor, the DAG parsing will be done by the scheduler itself, because the scheduler is the brain. If there is a DAG Processor, the DAG Processor will take it first — it pulls the DAG file from the DAGs folder and parses it.

**3.** Then it gets the blueprint of the DAG and saves that blueprint in the metadata database. You don't save the DAG in the metadata database — it saves the *blueprint* of the DAG.

**4.** The schedule for when the DAG runs should happen is also stored in the metadata database. That is handled by the scheduler. The scheduler stores everything, it writes everything in the metadata database about the DAG runs.

> So that is **parsed** and then **scheduled**.

**5.** Then the scheduler asks the executor to run the task.

**6.** The executor will split these tasks accordingly, depending on which worker is actually available to run the task. If it is Celery, then it splits the work; if it's Local, it just executes on that same machine.

**7.** The executor sends those tasks — or splits those tasks — to the workers, if there are any workers. If it's Local Executor or Kubernetes, it executes according to that executor's model instead of using Celery workers.

**8.** Then the logs generated by the DAG runs will be written to the metadata database by the worker. The task runs can be failed runs or successful runs — all the logs and task state will be written back to the metadata database.

**9.** After everything happens, the Web Server reads from the metadata database and shows us the updates in the UI, the user interface.

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
| **Scheduler** | Decision maker | Decides *when* to run. It doesn't run the task. |
| **Executor** | Launcher | Launches the task, but doesn't run it. Splits the tasks between the workers that are actually available. |
| **Worker** | Runner | The one who actually runs the task. |
| **Metadata Database** | Memory | Airflow's personal database — stores the logs, task state, DAG state, run history, schedules, and retries. |
| **Web Server / API Server** | Visibility | A dashboard to show what's happening. Doesn't participate in execution. |

The scheduler is a decision maker — it just decides when to run. The executor is the one who launches the task, but it doesn't run the task. The worker is the one who runs the task.

**Executor is a launcher, worker is a runner.**

Even without the Web Server we can execute everything — we just wouldn't be able to see what's happening. But the Web Server makes it easier for us to monitor the DAG runs.

---

## Executors

Now let's dive into executors.

The types of executors include **Local Executor**, **Celery Executor**, **Kubernetes Executor**, and there is also **Sequential Executor**. Sequential is so closely identical to Local Executor that there is only one slight difference between them.

**The type of executor doesn't change the tasks or the logic of the task. It only changes how these tasks will execute.** That's it.

An executor lets us scale horizontally whenever we add a new component to this architecture. What the executor does is scale: it has more workers to work in parallel on the same task. So it scales horizontally.

### Local Executor

This is the **simplest executor**.

First, we have the scheduler at the top. Then we have the Local Executor.

Local Executor doesn't have any workers — it doesn't have any *extra* workers, it just has one worker. This architecture is so simple because it just has one worker, on the same machine. The worker *is* the same machine.

In this executor, the scheduler and the worker — all of the setup — lives on the same machine. We don't split the work across different machines. Everything is just native on the same machine.

But Local Executor is **not production grade**, which means when you work in real companies you don't see Local Executor. What Local Executor is useful for is students who learn, or for local testing. We use Local Executor because it's so simple to set up, but all tasks that run in this executor run on the same machine, so it won't divide the work.

That's why the architecture is simple — you can easily set up the infrastructure for that and run it. This is good for learning, testing, or development, but it's not good for production grade.

### Sequential Executor

The difference between the Local Executor and the Sequential Executor is that in Local Executor **all the tasks run in parallel**, whereas in Sequential Executor it **only runs one task at a time**. The next task will wait until the task that is running is successfully executed.

That's the difference between the two. The rest of the architecture is identical to Local Executor.

### Celery Executor

The Celery Executor is quite different.

The tasks also start from the scheduler. Like any other system, we get the DAG plan — the blueprint — from the scheduler. Then we have the Celery Executor at the bottom of the scheduler, and underneath this we are going to have a **message broker**.

The tasks we want to execute will be transferred from the Celery Executor to the workers through the message broker.

Now it's not one single worker, it's **multiple workers** — it could be any number of workers, and we can configure them according to our needs or our load. So as distributed workers, this work will be distributed throughout the workers that are available, through the message broker. It delivers the message from the executor to the worker. That is the role of the message broker.

One more point I have to add: this executor is a **classic production grade executor**, and it is also stable and horizontally scalable.

With Local Executor we cannot scale horizontally, whereas with Celery Executor we can — because we can use multiple workers for the same DAG file, and we can assign the tasks in the DAG file to different workers. That's why this is horizontally scalable.

### Kubernetes Executor

Unlike the Celery Executor, the **Kubernetes Executor eliminates the use of a message broker**. We don't need a message broker in this executor's architecture, so it can access the worker right away.

The Celery Executor is production grade and works well for constant workloads. But what's different in Kubernetes Executor is that it is **modern and cloud-native**. We don't use VMs, virtual machines, or physical machines — with Kubernetes everything is cloud-native, and that's why it is modern and it is also **auto-scalable**.

It can start and close the pods according to the load. Let's say there are three tasks — it opens three pods. **Pods are nothing but workers**, but in Kubernetes we call them pods. Pods are actually workers, like in Celery.

So instead of having a particular number of workers working constantly, those workers get created or eliminated according to our need. That reduces the **billing cost** — because Kubernetes is cloud-native, the cloud platform that we use to initialize it charges us a certain amount, and that amount actually depends on how long we run and the number of pods. That's why auto-scaling is a good feature: only the pods that are required for the load we have will be started, and the rest will be eliminated.

The thing with Kubernetes is that it is **more powerful than Celery — in theory**. But it's so complex: it is operationally complex, and it is harder to set up and manage than the Celery Executor. Still, it's modern, and it is better than Celery Executor in a lot of factors.

Here's a good example of the difference. Let's say the DAG has a lot of tasks and we are on Celery Executor. We can assign the tasks to the workers, but if we exceed the load limit of those workers, the task has to **wait in the queue** until those workers become available. Whereas in Kubernetes, it doesn't need to wait for any of the pods — it just creates a new one for that particular task, and eliminates it after the task is done.

That is a big feature in Kubernetes Executor.

---

## Executor Comparison

So, to summarize what I was saying:

| | **Local Executor** | **Celery Executor** | **Kubernetes Executor** |
| --- | --- | --- | --- |
| **How it runs** | Runs on the same machine | Distributed workers | One pod per task |
| **Message broker** | Not needed | Required (Redis, RabbitMQ) | Not needed |
| **Scalability** | Low | Medium to high | Very high (auto-scaling) |
| **Setup complexity** | Very low | Medium | High |
| **Typical use** | Learning, testing, development | Classic production model | Modern and cloud-native production |

**Scalability.** The scalability for Local Executor is low, because we only use one worker — that is one machine for every component. We are not even going to split those components across machines, we just run all the components on one single machine, so the scalability is so low.

Celery Executor is from medium to high. It's not *at* the high, because we can only scale it to a specific number of workers.

Whereas Kubernetes is always ready for heavy loads or unexpected heavy loads that are coming in, and it is also best for that type of situation.

**Saving computational power.** Kubernetes is also the best at saving computational power. Let's say you make 100 workers run every day — even if the data load only needs 30 workers, you still have to keep all 100 running, because you don't know what can happen.

But in Kubernetes, you don't control the number of workers you hold, because Kubernetes handles it all. You don't do anything. When the load increases, the number of pods increases; when the load decreases, the extra pods are eliminated and the number of pods decreases automatically. So yes, it is very good for scalability — auto-scalable — but with more complexity.

**Complexity.** Local Executor is very low complexity, it's so easy to set up. The Celery Executor is complicated, because we also need to set up the workers on different machines — they can be virtual machines or actual physical machines — so the setup is quite a bit more complex than Local Executor. And then there is Kubernetes, which is high complexity for setup, but it's worth it.

---

## Installation Note

We're going to use the **Local Executor** for learning, installed with **Docker**.

There are a lot of ways to install Airflow on your PC and we can do it quite easily, but I'm choosing to install Airflow as a Docker image because it is a lot cleaner than installing it right on your local PC. Once you're done with the learning process, if you want to get rid of the files that Airflow created, that's a hard job to do.

Docker is a lot easier than a local installation, so I'm going with the Docker one here.
