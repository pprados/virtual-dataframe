# -*- coding: utf-8 -*-
"""
Tools for machine learning pipeline.
"""
import glob
import logging
import os
from pathlib import Path
from typing import Type, Optional, List, Any

import click

# Expose the type of model for all python packages
Model: Any = Any  # TODO: Select type


def init_logger(logger: logging.Logger, level: int) -> None:
    """ Init logger

    :param logger The logger to initialize
    :param level The level
    """
    # See https://tinyurl.com/4xmcyznj
    logging.getLogger().setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)


class Glob(click.ParamType):
    """
    A Click path argument that returns a ``List[Path]`` via un glob syntax, not a string.
    """

    def __init__(self, exists=False, recursive=False, default_suffix="*") -> None:
        self.exists = exists
        self.default_suffix = default_suffix
        self.recursive = recursive

    def convert(
            self,
            value: str,
            param: Optional[click.core.Parameter],
            ctx: Optional[click.core.Context],
    ) -> List[Path]:
        """
        Return a ``Path`` from the string ``click`` would have created with
        the given options.
        """
        if "*" not in value:
            value = os.path.join(value, self.default_suffix)
        return [Path(x) for x in
                glob.glob(super().convert(value=value, param=param, ctx=ctx),
                          recursive=self.recursive)]

# TODO: Add common code here
