from ._version import __version__
__author__ = 'Philippe PRADOS'
__email__ = 'github@prados.fr'

from typing import List

from dotenv import load_dotenv

from .env import DEBUG, VDF_MODE, Mode
from .vclient import VClient
from .vlocalcluster import VLocalCluster
from .vpandas import BackEndDataFrame, BackEndSeries, BackEnd, FrontEnd
from .vpandas import VDataFrame, VSeries, visualize
from .vpandas import compute, concat, delayed, persist
from .vpandas import from_pandas, from_backend
from .vpandas import read_csv, read_excel, read_feather, read_fwf, read_hdf
from .vpandas import read_json, read_orc, read_parquet, read_sql_table


load_dotenv()

__all__: List[str] = [
    'DEBUG', 'VDF_MODE', 'Mode',
    'VDataFrame', 'VSeries', 'VClient', 'VLocalCluster',
    'FrontEnd',
    'BackEndDataFrame', 'BackEndSeries', 'BackEnd',
    'compute', 'concat', 'delayed', 'persist', 'visualize',
    'from_pandas', 'from_backend',
    'read_csv', 'read_excel', 'read_feather', 'read_fwf', 'read_hdf',
    'read_json', 'read_orc', 'read_parquet', 'read_sql_table'
]
