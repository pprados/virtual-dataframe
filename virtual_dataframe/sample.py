from virtual_dataframe import delayed,VDataFrame


TestDF = VDataFrame


@delayed
def sample_function(data: TestDF) -> TestDF:
    return data
