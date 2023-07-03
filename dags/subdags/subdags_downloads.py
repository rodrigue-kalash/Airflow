from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 



def subdag_downloads(parents_dag_id, child_dag_id , args):
    with DAG(
        f"{parents_dag_id}.{child_dag_id}",
        start_date = args['start_date'],
        schedule_interval = args ['schedule_interval'],
        catchup = args['catchup'] )as dag:
    
        download_a = BashOperator(
            task_id='download_a',
            bash_command = 'sleep10',

        )
        
        download_b = BashOperator(
            task_id='download_b',
            bash_command = 'sleep10',

        )
        download_c = BashOperator(
            task_id='download_c',
            bash_command = 'sleep10',

        )
    return dag  