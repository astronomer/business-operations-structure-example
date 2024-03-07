Use case Business Operations - A structure repository for complex DAGs powering a user-facing dashboard
=======================================================================================================

This repository contains an example of an ETL/ELT structure with several transformation layers that could power a user-facing dashboard.

To make this repository accessible to everyone, the operators were mocked to not actually perform any operations and do not need a real Snowflake connection. The repository is meant to be a starting point for anyone looking to build a complex data pipeline with Airflow to power their business operations.

# How to use this repository

This section explains how to run this repository with Airflow. Note that you will need to copy the contents of the `.env_example` file to a newly created `.env` file. You do not need to provide real Snowflake credentials, as the operators are mocked.

Download the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) to run Airflow locally in Docker. `astro` is the only package you will need to install locally.

1. Run `git clone https://github.com/astronomer/use-case-business-operations-structure.git` on your computer to create a local clone of this repository.
2. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli). Docker Desktop/Docker Engine is a prerequisite, but you don't need in-depth Docker knowledge to run Airflow with the Astro CLI.
3. Run `astro dev start` in your cloned repository.
4. After your Astro project has started. View the Airflow UI at `localhost:8080`.
5. Unpause all DAGs to see a fully data-driven run of the repository.