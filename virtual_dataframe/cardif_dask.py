import sys

import click
import dotenv

from virtual_dataframe.tools import init_logger, logging
from typing import Any

LOGGER: logging.Logger = logging.getLogger(__name__)


@click.command(short_help="Sample to use dask")
def main():
    pass  # TODO


if __name__ == '__main__':
    init_logger(LOGGER, logging.INFO)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    if not hasattr(sys, 'frozen') and hasattr(sys, '_MEIPASS'):
        dotenv.load_dotenv(dotenv.find_dotenv())

    sys.exit(main())  # pylint: disable=E1120
