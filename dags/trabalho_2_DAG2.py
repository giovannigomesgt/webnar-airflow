from asyncio import Task
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
import pandas as pd
from datetime import datetime

default_args = {
    'owner': "Giovanni",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 12)
}

@dag(
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['Trabalho_2','Titanic', 'DAG2', 'Dynamic'])

def trabalho2_dag2():
    end =  DummyOperator(task_id = "END" )


    @task
    def read_data():
        # Ler a tabela única de indicadores feitos na Dag1 (/tmp/tabela_unica.csv)
        nome_do_arquivo = "/tmp/tabela_unica.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        print(df)
        return nome_do_arquivo


    @task
    def imrimir_logs(dataframe):
        # Produzir médias para cada indicador considerando o total
        nome_do_arquivo = dataframe
        df = pd.read_csv(dataframe, sep=";")        
        df2 = df.groupby(['Sex']).agg({"PassengerId": "sum","Fare": "mean","SibSp_and_Parch": "mean"}).reset_index()
        print(df2)
        return nome_do_arquivo

    @task
    def save_dataframe(data):
        # Escrever o resultado em um arquivo csv local no container (/tmp/resultados.csv)
        df = pd.read_csv(data, sep=";")    
        df.to_csv('/tmp/resultados.csv', index=False, sep=";")

    
        

    extrac = read_data()
    tarefa_1 = imrimir_logs(extrac)
    save_docker = save_dataframe(tarefa_1)
   

    extrac >> tarefa_1 >> save_docker >> end #>>  extrac >> transformacao >> [task_1, task_2, task_3] >> unificacao >> task_4 >> task_5


execucao = trabalho2_dag2()