from ._version import __version__

__author__ = 'Philippe PRADOS'
__email__ = 'github@prados.fr'

from typing import List

from dotenv import load_dotenv

from virtual_dataframe.env import DEBUG, VDF_MODE, Mode
from virtual_dataframe.vclient import VClient
from virtual_dataframe.vlocalcluster import VLocalCluster
from virtual_dataframe.vpandas import BackEndDataFrame, BackEndSeries, BackEndPandas
from virtual_dataframe.vpandas import FrontEndPandas
from virtual_dataframe.vpandas import VDataFrame, VSeries
from virtual_dataframe.vpandas import compute, concat, delayed, persist, visualize
from virtual_dataframe.vpandas import from_pandas, from_backend
from virtual_dataframe.vpandas import read_csv, read_excel, read_feather, read_fwf, read_hdf
from virtual_dataframe.vpandas import read_json, read_orc, read_parquet, read_sql_table

load_dotenv()

__all__: List[str] = [
    'DEBUG', 'VDF_MODE', 'Mode',
    'VDataFrame', 'VSeries', 'VClient', 'VLocalCluster',
    'FrontEndPandas',
    'BackEndDataFrame', 'BackEndSeries', 'BackEndPandas',
    'compute', 'concat', 'delayed', 'persist', 'visualize',
    'from_pandas', 'from_backend',
    'read_csv', 'read_excel', 'read_feather', 'read_fwf', 'read_hdf',
    'read_json', 'read_orc', 'read_parquet', 'read_sql_table',
]
