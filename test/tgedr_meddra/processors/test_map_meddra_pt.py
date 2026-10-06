from test.conftest import assert_frames_are_equal
from typing import Dict

import pytest
from pyspark.sql import DataFrame, Row

from nn.gs.ss.meddra.processors.map_meddra_pt import MapMedDRAPt


@pytest.fixture
def context(spark) -> Dict[str, DataFrame]:
    return {
        MapMedDRAPt.CONTEXT_KEY_PT: spark.createDataFrame(
            [
                Row(pt_code=10000002, pt_soc_code=10010331, pt="genetic disorders", version="25.1"),
                Row(pt_code=10000003, pt_soc_code=10010331, pt="genetic disorders a", version="25.1"),
                Row(pt_code=10000005, pt_soc_code=10022891, pt="17 ketosteroids urine", version="25.1"),
                Row(pt_code=10000007, pt_soc_code=10022891, pt="17 ketosteroids urine decreased", version="25.1"),
            ]
        ),
        MapMedDRAPt.CONTEXT_KEY_PT_SMQ_NNMQ_AGG_MAPPING: spark.createDataFrame(
            [
                Row(pt_code=10000002, smq_codes=[1.0], nnmq_codes=[11]),
                Row(pt_code=10000007, smq_codes=[2.0, 3.0], nnmq_codes=None),
                Row(pt_code=10000005, smq_codes=None, nnmq_codes=[12]),
            ]
        ),
        MapMedDRAPt.CONTEXT_KEY_LLT_FORMER_PT: spark.createDataFrame(
            [
                Row(llt_code=20000002, llt_name="genetic disorders b", pt_code=10000002, pt_soc_code=10010331),
            ]
        ),
    }


@pytest.fixture
def expected(spark) -> Dict[str, DataFrame]:
    return {
        MapMedDRAPt.OUTPUT_KEY_PT: spark.createDataFrame(
            [
                Row(
                    pt_code=10000002,
                    pt_soc_code=10010331,
                    pt="genetic disorders",
                    version="25.1",
                    smq_codes=[1.0],
                    nnmq_codes=[11],
                ),
                Row(
                    pt_code=10000003,
                    pt_soc_code=10010331,
                    pt="genetic disorders a",
                    version="25.1",
                    smq_codes=None,
                    nnmq_codes=None,
                ),
                Row(
                    pt_code=10000005,
                    pt_soc_code=10022891,
                    pt="17 ketosteroids urine",
                    version="25.1",
                    smq_codes=None,
                    nnmq_codes=[12],
                ),
                Row(
                    pt_code=10000007,
                    pt_soc_code=10022891,
                    pt="17 ketosteroids urine decreased",
                    version="25.1",
                    smq_codes=[2.0, 3.0],
                    nnmq_codes=None,
                ),
            ]
        ),
        MapMedDRAPt.OUTPUT_KEY_LLT_FORMER_PT: spark.createDataFrame(
            [
                Row(
                    llt_code=20000002,
                    llt_name="genetic disorders b",
                    pt_code=10000002,
                    pt_soc_code=10010331,
                    smq_codes=[1.0],
                    nnmq_codes=[11],
                ),
            ]
        ),
    }


def test_process(mocker, context, expected):
    o = MapMedDRAPt()

    actual: Dict[str, DataFrame] = o.process(context)
    assert_frames_are_equal(
        actual[MapMedDRAPt.OUTPUT_KEY_PT].toPandas(),
        expected[MapMedDRAPt.OUTPUT_KEY_PT].toPandas(),
        sort_columns=["pt_code"],
    )
    assert_frames_are_equal(
        actual[MapMedDRAPt.OUTPUT_KEY_LLT_FORMER_PT].toPandas(),
        expected[MapMedDRAPt.OUTPUT_KEY_LLT_FORMER_PT].toPandas(),
        sort_columns=["pt_code"],
    )
