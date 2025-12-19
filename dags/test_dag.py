from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import time

with DAG(
    dag_id = 'test_dag',
    start_date=datetime.now()
):
    def printtt():
        print('yay')
        print('t')

    run_this = PythonOperator(
        task_id='print_task',
        python_callable=printtt
    )


    run_this1 = PythonOperator(
        task_id='print_task1',
        python_callable=printtt
    )

    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(random_base)

    for i in range(5):
        sleeping_task = PythonOperator(
            task_id=f"sleep_for_{i}",
            python_callable=my_sleeping_function,
            op_kwargs={"random_base": i / 10}
        )

        run_this >> sleeping_task >> run_this1


