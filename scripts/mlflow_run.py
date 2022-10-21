import dvc.api
import mlflow
import pandas as pd
from log_supp import App_Logger

app_logger = App_Logger("logs/mlflow_run.log").get_app_logger()

path = 'data/df_order.csv'
repo = '.'
version = 'V2.0'

logger = App_Logger("logs/mlflow_run.log").get_app_logger()

logger.info('mlflow run started:')
data_url = dvc.api.read(
    path=path,
    rev=version,
    repo=repo
)
experiment = 'data_version'
mlflow.set_experiment(experiment)
mlflow.autolog()

with mlflow.start_run(experiment_id='1') as run:
    
    try:
        data = pd.read_csv(data_url)
    except Exception:
        logger.exception('Failed to Fetch Data: ')

        mlflow.log_param('data_version',version)
        mlflow.log_param('input_rows',data.shape[0])
        mlflow.log_param('input_columns',data.shape[1])







logger.info('mlflow run finished successfully:')
