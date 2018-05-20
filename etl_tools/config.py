"""Configuration of shared ETL utilities"""

import datetime

from mara_config import declare_config

@declare_config()
def first_date_in_time_dimensions() -> datetime.date:
    """The first date that should appear in time dimensions"""
    return datetime.date.today() - datetime.timedelta(days=365)


@declare_config()
def last_date_in_time_dimensions() -> datetime.date:
    """The last date that should appear in time dimensions"""
    return datetime.date.today()


@declare_config()
def number_of_chunks() -> int:
    """Big tables and computations are split into this many chunks"""
    return 7
