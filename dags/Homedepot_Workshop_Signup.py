from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

# Dummy signup data
SIGNUP_DATA = {
    "firstName": "John",
    "lastName": "Doe",
    "email": "john.doe@example.com",
    "zipCode": "12345",
    # Add other required fields as needed
}

WORKSHOP_URL = "https://www.homedepot.com/c/kids-workshop"


def fetch_workshop_page(**context):
    response = requests.get(WORKSHOP_URL)
    response.raise_for_status()
    context["ti"].xcom_push(key="workshop_html", value=response.text)


def parse_signup_form(**context):
    html = context["ti"].xcom_pull(key="workshop_html")
    soup = BeautifulSoup(html, "html.parser")
    # Find the signup form (update selector as needed)
    form = soup.find("form")
    if not form:
        raise Exception("Signup form not found")
    action = form.get("action")
    # Collect hidden inputs
    data = SIGNUP_DATA.copy()
    for inp in form.find_all("input", {"type": "hidden"}):
        data[inp.get("name")] = inp.get("value")
    context["ti"].xcom_push(key="form_action", value=action)
    context["ti"].xcom_push(key="form_data", value=data)


def submit_signup(**context):
    action = context["ti"].xcom_pull(key="form_action")
    data = context["ti"].xcom_pull(key="form_data")
    if not action or not data:
        raise Exception("Form action or data missing")
    # If action is relative, build full URL
    if action.startswith("/"):
        url = "https://www.homedepot.com" + action
    else:
        url = action
    response = requests.post(url, data=data)
    response.raise_for_status()
    print("Signup submitted, response:", response.text[:200])


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="homedepot_kids_workshop_signup",
    default_args=default_args,
    description="Sign up for Home Depot Kids Workshop",
    schedule=None,
    start_date=datetime(2025, 10, 9),
    catchup=False,
    tags=["homedepot", "workshop", "signup"],
) as dag:

    fetch_page = PythonOperator(
        task_id="fetch_workshop_page",
        python_callable=fetch_workshop_page,
    )

    parse_form = PythonOperator(
        task_id="parse_signup_form",
        python_callable=parse_signup_form,
    )

    # submit = PythonOperator(
    #    task_id='submit_signup',
    #    python_callable=submit_signup,
    # )

    fetch_page >> parse_form  # >> submit
