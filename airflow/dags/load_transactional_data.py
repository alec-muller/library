from datetime import datetime

from include.ingest_data import GenerateTransaction

from airflow.decorators import dag, task

default_args = {"owner": "Alec", "retries": 1, "retry_delay": 1}


@dag(
    dag_id="load_transactional_data",
    start_date=datetime(2024, 3, 26, 20),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
)
def main():
    """
    The mainly purpose of this DAG is to load transactional data into MySQL database,
    simulating a real-world scenario about renting books in a library.
    It consists of four tasks:
    1. Ingest random book information (title, author, etc.) from a real and open API:
        https://openlibrary.org/developers/api
    2. Ingest data from author's that have written the above mentioned books, from API.
    3. Ingest random customer information using Faker library.
    4. Generate random transactions for customers with rented books.
    """

    # Get MySQL's connection details as environment variables from Airflow
    v_host = "{{ var.value.mysql_host}}"
    v_user = "{{ var.value.mysql_user}}"
    v_password = "{{ var.value.mysql_password}}"
    v_port = "{{ var.value.mysql_port}}"
    v_db = "{{ var.value.mysql_db}}"

    ## Tasks definition session
    @task
    def books(host, user, password, port, db):
        GenerateTransaction(host, user, password, port, db).get_books()

    @task
    def authors(host, user, password, port, db):
        GenerateTransaction(host, user, password, port, db).get_authors()

    @task
    def customers(host, user, password, port, db):
        GenerateTransaction(host, user, password, port, db).get_customers()

    @task
    def rents(host, user, password, port, db):
        GenerateTransaction(host, user, password, port, db).get_rents()

    # Setting variables for tasks arguments
    t_books = books(v_host, v_user, v_password, v_port, v_db)
    t_authors = authors(v_host, v_user, v_password, v_port, v_db)
    t_customers = customers(v_host, v_user, v_password, v_port, v_db)
    t_rents = rents(v_host, v_user, v_password, v_port, v_db)

    # Setting dependencies between tasks
    t_books >> t_authors >> t_customers >> t_rents


main()
