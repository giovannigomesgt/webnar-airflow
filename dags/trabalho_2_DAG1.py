from asyncio import Task
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
#from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.trigger_rule import TriggerRule
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
    schedule_interval='@once',
    catchup=False,
    tags=['Trabalho_2','Titanic', 'DAG1'])

def trabalho2_dag1():
    inicio =  DummyOperator(task_id = "Start" )


    @task
    def read_write_data():
        #Ler os dados e escrever localmente dentro do container numa pasta /tmp
        URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")
        return NOME_DO_ARQUIVO

    transformacao =  DummyOperator(task_id = "Transform" )

    @task
    def task_1():
       # Quantidade de passageiros por sexo e classe (produzir e escrever)
        nome_tabela_1 = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv("/tmp/titanic.csv", sep=";")
        df1 = df.groupby(['Sex', 'Pclass']).agg({"PassengerId": "count"}).reset_index()
        print('-'*30)
        print(df1)
        print('-'*30)
        df1.to_csv(nome_tabela_1, index=False, sep=";")
        return nome_tabela_1

    
    @task
    def task_2():
        # Preço médio da tarifa pago por sexo e classe (produzir e escrever)
        nome_tabela_2 = "/tmp/preco_medio_por_sexoClasse.csv"
        df = pd.read_csv("/tmp/titanic.csv", sep=";")
        df2 = df.groupby(['Sex', 'Pclass']).agg({"Fare": "mean"}).sort_values(['Pclass','Sex'],ascending=False).reset_index()
        print('-'*30)
        print(df2)
        print('-'*30)
        df2.to_csv(nome_tabela_2, index=False, sep=";")
        return nome_tabela_2


    @task
    def task_3():
        # Quantidade total de SibSp + Parch (tudo junto) por sexo e classe (produzir e escrever)
        nome_tabela_3 = "/tmp/preco_medio_por_sexoClasse.csv"
        dfmodel = pd.read_csv("/tmp/titanic.csv", sep=";")
        dfmodel['SibSp_and_Parch'] = dfmodel['SibSp'] + dfmodel['Parch']
        df3 = pd.DataFrame(dfmodel, columns=['PassengerId', 'Sex', 'Pclass','SibSp_and_Parch'])
        df3 = df3.groupby(['Sex', 'Pclass']).agg({"SibSp_and_Parch": "count"}).reset_index()
        print('-'*30)
        print(df3)
        print('-'*30)
        df3.to_csv(nome_tabela_3, index=False, sep=";")
        return nome_tabela_3

    unificacao =  DummyOperator(task_id = "unify_tables" )

    @task
    def write_table_docker():
        # Juntar todos os indicadores criados em um único dataset (produzir o dataset e escrever) /tmp/tabela_unica.csv
        df = pd.read_csv("/tmp/titanic.csv", sep=";")
        df['SibSp_and_Parch'] = df['SibSp'] + df['Parch']
        print(df)
        tabela_unica = df.groupby(['Sex', 'Pclass']).agg({"PassengerId": "count", "Fare": "mean", "SibSp_and_Parch": "count"}).reset_index()
        print(tabela_unica)
        tabela_unica.to_csv('/tmp/tabela_unica.csv', index=False, sep=";")
        #return df

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    
    def end():
        pass
   
    
    triggerdag = TriggerDagRunOperator(
        task_id="trabalho2_dag2",
        trigger_dag_id="trabalho2_dag2")
    


    @task
    def read_table():
        df = pd.read_csv("/tmp/tabela_unica.csv", sep=";")
        print(df)

    extrac = read_write_data()
    task_1 = task_1()
    task_2 = task_2()
    task_3 = task_3()
    task_4 = write_table_docker()
    task_5 = read_table()
    inicio >>  extrac >> transformacao >> [task_1, task_2, task_3] >> unificacao >> task_4 >> task_5 >> triggerdag


execucao = trabalho2_dag1()