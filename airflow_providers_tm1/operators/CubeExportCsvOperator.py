import logging
import os
from typing import Callable

import pandas as pd
from TM1py import TM1Service
from TM1py.Utils import CaseAndSpaceInsensitiveSet

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow_providers_tm1.hooks.tm1 import TM1Hook


class CubeExportCsvOperator(BaseOperator):
    """
    This operator loads a csv file into a TM1 cube, with transformation function

    :param connection_id: The Airflow connection used for TM1 credentials.
    :type connection_id: str

    :param cube_name: The name of the cube to load the csv file into.
    :type cube_name: str

    :param csv_file_path: The path to the csv file to load.
    :type csv_file_path: str

    :param transform_callable: A callable that transforms the csv file into a cube favor pandas dataframe, the callable output must be pd.DataFrame.
    :type transform_callable: Callable

    :param mdx: The MDX query to execute.
    :type mdx: str

    :param cubeview: The name of the cubeview to use. [Optional] If cubeview and mdx are provided, error will be raised.
    :type cubeview: str

    :param execute_mdx_param: Additional parameters to pass to TM1py.cells.execute_mdx_dataframe.
    :type execute_mdx_param: dict[str, any]

    :param kwargs: Additional keyword arguments to pass to tm1py.cells.write_dataframe and transform_callable.
    :type kwargs: dict[str, any]
    """

    def __init__(
        self, connection_id, cube_name: str,  csv_file_path: str, transform_callable: Callable | None, mdx: str = '', cubeview: str = "", execute_mdx_param: dict = {}, *args, **kwargs
    ):

        super().__init__(
            *args,
            task_id=f"export-csv-from-{cube_name.replace(' ', '')}",
            task_display_name=f"Export {connection_id}.{cube_name} to {os.path.basename(csv_file_path)}",
            **kwargs,
        )

        self.hook = TM1Hook(tm1_conn_id=connection_id)
        self.csv_file_path = csv_file_path
        self.cube_name = cube_name
        self.transform_callable = transform_callable
        self.mdx = mdx 

        self.cubeview = cubeview
        if (not self.mdx and not self.cubeview) or (self.mdx and self.cubeview):
            raise ValueError("Either mdx or cubeview must be provided")
        self.execute_mdx_param = execute_mdx_param
        self.kwargs = kwargs

    def execute(self, context):
        logger = self.logger()
        logger.setLevel(logging.DEBUG)
        if not os.path.exists(self.csv_file_path):
            raise FileNotFoundError(f"File {self.csv_file_path} not found")

        logger.debug(f"file path exists")
        logger.debug(f"file size : {os.path.getsize(self.csv_file_path)}")

        with self.hook.get_conn() as tm1:
            if not tm1.cubes.exists(self.cube_name):
                raise ValueError(f"Cube {self.cube_name} not found")
            cubeview_mdx = ""
            if self.cubeview:
                if not tm1.views.exists(cube_name=self.cube_name, view_name=self.cubeview):
                    raise AirflowException(f"Cubeview {self.cubeview} not found in cube {self.cube_name}")
                cubeview_mdx = tm1.views.get(cube_name=self.cube_name, view_name=self.cubeview).MDX
            
            mdx = self.mdx or cubeview_mdx

            cell_count = tm1.cells.execute_mdx_cellcount(self.mdx)
            logger.debug(f"cell count: {cell_count} with mdx: \n{mdx}")

            logger.debug(f"executing mdx with parameters: {self.execute_mdx_param}")
            df = tm1.cells.execute_mdx_dataframe(mdx, **self.execute_mdx_param,)
            logger.debug(f"raw dataframe shape : {df.shape}")
            logger.debug(f"memory usage: \n{df.memory_usage(index=False)}")
            logger.debug(f"dataframe head: \n{df.head()}")

            if callable(self.transform_callable):
                df = self.transform_callable(df, **self.kwargs)

            df.to_csv(self.csv_file_path, index=False, encoding='utf-8')
    