# Indicium Tech Code Challenge
# Data Pipeline


## Context

This is a data pipeline project built with Apache Airflow to process input data and generate useful insights. The pipeline was developed to run in a Python 3.x environment.

## Prerequisites

Make sure you have Python 3.x installed on your system. You can download and install Python from the official Python website.


## Technologies Used

- Python (Version: 3.9.9)
- Apache Airflow (Version: 2.7.2)
- Embulk (Version: 0.10.27)
- Docker (Version: 20.10.11)
- Docker Compose (Version: 1.29.2)

## ⚙️ Running the Pipeline Locally

To run the data pipeline locally using Docker and Apache Airflow, follow these steps:

1. Clone the repository:

    ```
    git clone https://github.com/vlruiz108/LH_ED_VANESSA_RUIZ
    ```

2. Navigate to the project directory:

    ```
    cd data-pipeline
    ```
    ```bash
    python data_pipeline.py
    ```

3. Create and activate a virtual environment (optional but recommended):

    ```
    python -m venv venv
    source venv/bin/activate  # on Windows use venv\Scripts\activate.bat
   ```

4. Install project dependencies:

    ```
    pip install -r requirements.txt
    ```

5. Configure the necessary credentials and parameters in the `config.py` file. You can create a copy of the example file `config_example.py` and rename it to `config.py`.

6. Ensure that Apache Airflow is configured correctly. You can refer to the official [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html) for detailed instructions on configuration.

7. Build and start the Docker containers:

```bash
    docker-compose up --build
```

8. To lift the containers:
```
     docker-compose up -d
```
9. Consult the container

    ```
    docker-compose ps
    ```    


10. Start Apache Airflow:

    ```
    airflow webserver --port 8080
    ```

    and in another terminal:

    ```
    airflow scheduler
    ```

11. Access the Airflow dashboard at http://localhost:8080 in your web browser.

12. Activate the DAG (Directed Acyclic Graph) `data_pipeline` in the Airflow dashboard.

13. The pipeline is now configured to run according to the schedule defined in the DAG.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
