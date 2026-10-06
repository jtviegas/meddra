from datetime import datetime, timezone
from test.conftest import assert_frames_equality
from typing import Dict

import pytest
from pyspark.sql import DataFrame, Row

from nn.gs.ss.meddra.processors.meddra_llt_former_pt import MedDRALltFormerPt

NOW: int = int(datetime.now(tz=timezone.utc).timestamp())


@pytest.fixture
def datasets(spark) -> Dict[str, DataFrame]:
    return {
        "meddra_history_english.asc": spark.createDataFrame(
            [
                Row(
                    term_code=10000001,
                    term_name="11-beta-hydroxylase deficiency A",
                    term_addition_version=2.1,
                    term_type="PT",
                    llt_currency=None,
                    action="A",
                ),
                Row(
                    term_code=20000001,
                    term_name="11-beta-hydroxylase deficiency A",
                    term_addition_version=2.1,
                    term_type="LLT",
                    llt_currency="Y",
                    action="A",
                ),
                Row(
                    term_code=10000002,
                    term_name="genetic disorders",
                    term_addition_version=12.1,
                    term_type="PT",
                    llt_currency=None,
                    action="A",
                ),
                Row(
                    term_code=20000002,
                    term_name="genetic disorders",
                    term_addition_version=12.1,
                    term_type="LLT",
                    llt_currency="Y",
                    action="A",
                ),
                Row(
                    term_code=10000002,
                    term_name="genetic disorders",
                    term_addition_version=14.1,
                    term_type="PT",
                    llt_currency=None,
                    action="D",
                ),
                Row(
                    term_code=10000003,
                    term_name="gastric disorders",
                    term_addition_version=12.1,
                    term_type="PT",
                    llt_currency=None,
                    action="A",
                ),
                Row(
                    term_code=20000003,
                    term_name="gastric disorders",
                    term_addition_version=12.1,
                    term_type="LLT",
                    llt_currency="Y",
                    action="A",
                ),
                Row(
                    term_code=10000003,
                    term_name="gastric disorders",
                    term_addition_version=14.1,
                    term_type="PT",
                    llt_currency=None,
                    action="D",
                ),
                Row(
                    term_code=20000003,
                    term_name="gastric disorders",
                    term_addition_version=14.1,
                    term_type="LLT",
                    llt_currency=None,
                    action="D",
                ),
                Row(
                    term_code=10010331,
                    term_name="Congenital and familial/genetic disorders",
                    term_addition_version=2.1,
                    term_type="SOC",
                    llt_currency=None,
                    action="D",
                ),
            ]
        ),
        "llt.asc": spark.createDataFrame(
            [
                Row(
                    llt_code=20000002,
                    pt_code=10000012,
                ),
                Row(
                    llt_code=20000008,
                    pt_code=10000018,
                ),
            ]
        ),
        "pt.asc": spark.createDataFrame(
            [
                Row(
                    pt_code=10000012,
                    pt_soc_code=30000012,
                ),
                Row(
                    pt_code=10000018,
                    pt_soc_code=30000018,
                ),
            ]
        ),
    }


@pytest.fixture
def expected(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(llt_code=20000002, llt_name="genetic disorders", pt_code=10000012, pt_soc_code=30000012),
        ]
    )


def test_process(datasets, expected):
    o: MedDRALltFormerPt = MedDRALltFormerPt()
    actual = o.process({"datasets": datasets})

    assert_frames_equality(actual.toPandas(), expected.toPandas(), ["llt_code"])
