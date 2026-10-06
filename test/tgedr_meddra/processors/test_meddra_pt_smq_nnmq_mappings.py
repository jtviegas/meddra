from test.conftest import assert_frames_are_equal
from typing import Dict

import pandas as pd
import pyspark.sql.types as T
import pytest
from pyspark.sql import DataFrame, Row

import nn.gs.ss.meddra.processors.meddra_pt_smq_nnmq_mappings as mpm
from nn.gs.ss.meddra.processors.meddra_pt_smq_nnmq_mappings import (
    MedDRAPtSmqNNmqMappings,
)


@pytest.fixture
def expected_pt_nnmq(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(pt_code=10000002, nnmq_code=11),
            Row(pt_code=10000005, nnmq_code=12),
        ]
    )


@pytest.fixture
def expected_pt_smq(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(pt_code=10000002, smq_code=1.0),
            Row(pt_code=10000007, smq_code=2.0),
            Row(pt_code=10000007, smq_code=3.0),
        ]
    )


@pytest.fixture
def expected_pt_nnmq_smq_agg(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(pt_code=10000002, smq_codes=[1.0], nnmq_codes=[11]),
            Row(pt_code=10000007, smq_codes=[2.0, 3.0], nnmq_codes=None),
            Row(pt_code=10000005, smq_codes=None, nnmq_codes=[12]),
        ]
    )


@pytest.fixture
def mappings(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(CATEGORY_SERIAL_NUMBER=11, SMQ_CODE=1.0, NNMQ_PT_CODE=10000002),
            Row(CATEGORY_SERIAL_NUMBER=12, SMQ_CODE=None, NNMQ_PT_CODE=10000005),
            Row(CATEGORY_SERIAL_NUMBER=0, SMQ_CODE=2.0, NNMQ_PT_CODE=10000007),
            Row(CATEGORY_SERIAL_NUMBER=0, SMQ_CODE=3.0, NNMQ_PT_CODE=10000007),
        ],
        schema=T.StructType(
            [
                T.StructField("CATEGORY_SERIAL_NUMBER", T.LongType(), True),
                T.StructField("SMQ_CODE", T.DoubleType(), True),
                T.StructField("NNMQ_PT_CODE", T.LongType(), True),
            ]
        ),
    )


@pytest.fixture
def src_mappings() -> pd.DataFrame:
    return pd.DataFrame.from_dict(
        {
            "NNMQ_SEQ_NO": {0: 1, 1: 2, 2: 3, 3: 4},
            "NNMQ_PT_NAME": {
                0: "Absence of immediate treatment response",
                1: "Acquired generalised lipodystrophy",
                2: "Alpha hydroxybutyric acid increased",
                3: "Alpha hydroxybutyric acid increaseds",
            },
            "SHOW_CATEGORY": {
                0: "NMQ_HypoHyperLoe",
                1: "NMQ_HypoHyperLoe",
                2: "NMQ_HypoHyperLoe",
                3: "NMQ_HypoHyperLoe",
            },
            "CATEGORY_SERIAL_NUMBER": {0: 11, 1: 12, 2: 0, 3: 0},
            "MAPPING_CATEGORY": {0: "HypoHyperLoe", 1: "HypoHyperLoe", 2: "HypoHyperLoe", 3: "HypoHyperLoe"},
            "SHOW_FILTER": {
                0: "NMQ_HypoHyperLoe_Narrow",
                1: "NMQ_HypoHyperLoe_Narrow",
                2: "NMQ_HypoHyperLoe_Narrow",
                3: "NMQ_HypoHyperLoe_Narrow",
            },
            "TERM_SCOPE_TEXT": {0: "Narrow", 1: "Narrow", 2: "Narrow", 3: "Narrow"},
            "TERM_SCOPE": {0: 2, 1: 2, 2: 2, 3: 3},
            "TERM_CATEGORY": {0: None, 1: None, 2: None, 3: None},
            "SMQ_ALGORITHM": {0: None, 1: None, 2: None, 3: None},
            "smq_name": {0: None, 1: None, 2: None, 3: None},
            "SMQ_CODE": {0: 1.0, 1: None, 2: 2.0, 3: 3.0},
            "hlgt_name": {0: None, 1: None, 2: None, 3: None},
            "hlt_name": {0: None, 1: None, 2: None, 3: None},
            "ART_CODE": {0: 10081766, 1: 10087376, 2: 10089543, 3: 10089599},
            "NNMQ_PT_CODE": {0: 10000002, 1: 10000005, 2: 10000007, 3: 10000007},
            "MEDDRA_VER": {0: 27.0, 1: 27.0, 2: 27.0, 3: 27.0},
        }
    )


def test_process(monkeypatch, src_mappings, expected_pt_nnmq_smq_agg, expected_pt_nnmq, expected_pt_smq):

    o = MedDRAPtSmqNNmqMappings()

    class MockCatalogFileSource:
        def list(self, context):
            return ["dummy"]

    monkeypatch.setattr(mpm, "CatalogFileSource", MockCatalogFileSource)

    class MockPdDfCatalogSource:
        def get(self, context):
            return src_mappings

    monkeypatch.setattr(mpm, "PdDfCatalogSource", MockPdDfCatalogSource)

    actual: Dict[str, DataFrame] = o.process(
        {
            MedDRAPtSmqNNmqMappings.CONTEXT_KEY_MAPPING_SRC_URL: "dummy",
        }
    )

    assert_frames_are_equal(
        actual[MedDRAPtSmqNNmqMappings.OUTPUT_KEY_PT_NNMQ_SMQ_AGG].toPandas(),
        expected_pt_nnmq_smq_agg.toPandas(),
        sort_columns=["pt_code"],
    )
    assert_frames_are_equal(
        actual[MedDRAPtSmqNNmqMappings.OUTPUT_KEY_PT_SMQ].toPandas(),
        expected_pt_smq.toPandas(),
        sort_columns=["pt_code"],
    )
    assert_frames_are_equal(
        actual[MedDRAPtSmqNNmqMappings.OUTPUT_KEY_PT_NNMQ].toPandas(),
        expected_pt_nnmq.toPandas(),
        sort_columns=["pt_code"],
    )
