# 3. Orchestration and ML Pipelines

This section demonstrates how to build, track, and orchestrate machine learning (ML) pipelines using **MLflow** for experiment tracking and **Prefect** for workflow orchestration.

## Overview

A typical ML pipeline involves several stages, such as data extraction, preprocessing, feature engineering, model training, and evaluation. Managing these steps manually can be error-prone and difficult to reproduce. By leveraging orchestration tools like Prefect and experiment tracking tools like MLflow, you can automate, monitor, and reproduce your ML workflows efficiently.

## ML Pipeline Example

A complete ML pipeline—covering data extraction, manipulation, and model training—is provided in the [notebook](./examples/instructor/duration_prediction.ipynb). For production or automation purposes, this notebook has been converted into a [Python script](./examples/instructor/duration_prediction.py).

## MLflow for Experiment Tracking

[MLflow](https://mlflow.org/) is used to track experiments, log metrics, and manage model versions. To start the MLflow UI and visualize your experiment runs, execute the following command in your terminal:

```bash
mlflow ui --port 1994
```

> **Note:** There was a typo in the previous command (`mflow` instead of `mlflow`). Make sure to use `mlflow`.

This will launch the MLflow tracking server at [http://127.0.0.1:1994](http://127.0.0.1:1994), where you can explore your experiment runs, parameters, and results.

## Prefect for Orchestration

[Prefect](https://www.prefect.io/) is a modern workflow orchestration tool that helps you automate and monitor your data and ML pipelines. Several tutorials are available in the [examples/learn_prefect/](./examples/learn_prefect/) directory, demonstrating how to convert Python scripts into Prefect flows.

To start the Prefect server and visualize your flows and runs, use:

```bash
prefect server start
```

Then, visit the Prefect UI at [http://127.0.0.1:4200/](http://127.0.0.1:4200/) to monitor and manage your workflows.

## Additional Resources

For more in-depth tutorials and resources on Prefect and orchestration in MLops, check out the following:

- [Getting started with Prefect (YouTube)](https://www.youtube.com/watch?v=D5DhwVNHWeU)
- [2024 MLops Orchestration (DataTalksClub)](https://github.com/DataTalksClub/mlops-zoomcamp/tree/main/03-orchestration)
- [2024 Prefect notes (DataTalksClub)](https://github.com/DataTalksClub/mlops-zoomcamp/blob/main/cohorts/2023/03-orchestration/prefect/README.md)

---

By following the examples and resources in this section, you will learn how to:

- Structure ML pipelines for reproducibility and automation
- Track experiments and results using MLflow
- Orchestrate and monitor workflows using Prefect

Feel free to explore the provided scripts and notebooks, and adapt them to your own ML projects!