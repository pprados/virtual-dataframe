import pandera
from virtual_dataframe import delayed
from typing import Any


class TestDF_schema(pandera.SchemaModel):
    id: pandera.typing.Index[int]
    data: pandera.typing.Series[int]

    class Config:
        strict: bool = True
        ordered: bool = True


TestDF: Any = pandera.typing.DataFrame[TestDF_schema]


@delayed
@pandera.check_types
def sample_function(data: TestDF) -> TestDF:
    return data
