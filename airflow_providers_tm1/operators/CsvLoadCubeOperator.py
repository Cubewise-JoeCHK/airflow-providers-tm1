from airflow.models import BaseOperator
from airflow_providers_tm1.hooks.tm1 import TM1Hook
from typing import Callable
import os
import pandas as pd
import logging
from TM1py import TM1Service
from TM1py.Utils import CaseAndSpaceInsensitiveSet
from airflow.exceptions import AirflowException

logging.basicConfig(level=logging.DEBUG)


class CsvLoadCubeOperator(BaseOperator):
  """
  This operator loads a csv file into a TM1 cube, with transformation function
  
  :param connection_id: The Airflow connection used for TM1 credentials.
  :type connection_id: str

  :param csv_file_path: The path to the csv file to load.
  :type csv_file_path: str

  :param cube_name: The name of the cube to load the csv file into.
  :type cube_name: str

  :param transform_callable: A callable that transforms the csv file into a cube favor pandas dataframe, the callable output must be pd.DataFrame.
  :type transform_callable: Callable

  :param kwargs: Additional keyword arguments to pass to tm1py.cells.write_dataframe and transform_callable.
  :type kwargs: dict[str, any]
  """

  def __init__(self, connection_id, csv_file_path:str, cube_name:str, transform_callable: Callable|None, *args, **kwargs):

    super().__init__(*args, task_id = f'load-csv-to-{cube_name.replace(' ', '')}', task_display_name=f'Load {os.path.basename(csv_file_path)} to {connection_id}.{cube_name}', **kwargs)

    self.hook = TM1Hook(tm1_conn_id=connection_id)
    self.csv_file_path = csv_file_path
    self.cube_name = cube_name
    self.transform_callable = transform_callable
    self.kwargs = kwargs

  def execute(self, context):
    logger = self.logger()
    logger.setLevel(logging.DEBUG)
    if not os.path.exists(self.csv_file_path):
      raise FileNotFoundError(f"File {self.csv_file_path} not found")
    
    logger.debug(f'file path exists')
    logger.debug(f'file size : {os.path.getsize(self.csv_file_path)}')

    with self.hook.get_conn() as tm1:
      if not tm1.cubes.exists(self.cube_name):
        raise ValueError(f"Cube {self.cube_name} not found")
      logger.debug(f'Cube dimension list : ')
      logger.debug(f'{tm1.cubes.get_dimension_names(self.cube_name)}')
      
      df = pd.read_csv(self.csv_file_path,dtype=str)
      logger.debug(f'raw dataframe shape : {df.shape}')
      logger.debug(f'memory usage: \n{df.memory_usage(index=False)}')
      logger.debug(f'dataframe head: \n{df.head()}')

      if callable(self.transform_callable):
        df = self.transform_callable(df, **self.kwargs)

      if not self.healthcheck(tm1, df):
        raise AirflowException(f"Healthcheck failed for cube {self.cube_name}, please check the logs for more information")
      
      tm1.cells.write_dataframe(self.cube_name, df, **self.kwargs)

    return True
  
  def healthcheck(self, tm1: TM1Service, df:pd.DataFrame) -> bool:
    logger = self.logger()
    healthcheck = True
    for data_col, tm1_dim in zip(df.columns, tm1.cubes.get_dimension_names(self.cube_name)):
      data_col = CaseAndSpaceInsensitiveSet(df[data_col].unique())
      tm1_ele = tm1.elements.get_all_element_identifiers(tm1_dim, tm1_dim)
      missing_elements = data_col - tm1_ele
      if missing_elements:
        logger.error(f"Missing elements in TM1 dimension {tm1_dim}: {missing_elements}")
        healthcheck = False

    return healthcheck