from datetime import datetime, timezone
from test.conftest import assert_frames_equality
from typing import Any, Dict, Optional

import pytest
from nn.gs.ss.dataops.source.source import SourceException
from numpy import nan
from pyspark.sql import DataFrame, Row

import nn.gs.ss.meddra.etl.meddra_pt_processing as mpp
from nn.gs.ss.meddra.etl.meddra_pt_processing import MedDRAPTProcessing

NOW: int = int(datetime.now(tz=timezone.utc).timestamp())


@pytest.fixture
def medra_files(spark) -> Dict[str, DataFrame]:
    return {
        "pt.asc": spark.createDataFrame(
            [
                Row(
                    pt_code=200001,
                    pt_name="genetic disorders",
                    null_field=nan,
                    pt_soc_code=300001,
                    pt_whoart_code=nan,
                    pt_harts_code=nan,
                    pt_costart_sym=nan,
                    pt_icd9_code=nan,
                    pt_icd9cm_code=nan,
                    pt_icd10_code=nan,
                    pt_jart_code=nan,
                ),
                Row(
                    pt_code=200002,
                    pt_name="11-beta-hydroxylase deficiency",
                    null_field=nan,
                    pt_soc_code=300002,
                    pt_whoart_code=nan,
                    pt_harts_code=nan,
                    pt_costart_sym=nan,
                    pt_icd9_code=nan,
                    pt_icd9cm_code=nan,
                    pt_icd10_code=nan,
                    pt_jart_code=nan,
                ),
                Row(
                    pt_code=200003,
                    pt_name="17 ketosteroids urine",
                    null_field=nan,
                    pt_soc_code=300003,
                    pt_whoart_code=nan,
                    pt_harts_code=nan,
                    pt_costart_sym=nan,
                    pt_icd9_code=nan,
                    pt_icd9cm_code=nan,
                    pt_icd10_code=nan,
                    pt_jart_code=nan,
                ),
                Row(
                    pt_code=200004,
                    pt_name="17 ketosteroids urine decreased",
                    null_field=nan,
                    pt_soc_code=300004,
                    pt_whoart_code=nan,
                    pt_harts_code=nan,
                    pt_costart_sym=nan,
                    pt_icd9_code=nan,
                    pt_icd9cm_code=nan,
                    pt_icd10_code=nan,
                    pt_jart_code=nan,
                ),
                Row(
                    pt_code=200007,
                    pt_name="cenas que doem",
                    null_field=nan,
                    pt_soc_code=300007,
                    pt_whoart_code=nan,
                    pt_harts_code=nan,
                    pt_costart_sym=nan,
                    pt_icd9_code=nan,
                    pt_icd9cm_code=nan,
                    pt_icd10_code=nan,
                    pt_jart_code=nan,
                ),
            ]
        ),
        "meddra_history_english.asc": spark.createDataFrame(
            [
                Row(
                    term_code=200004,
                    term_name="17 ketosteroids urine insufficient",
                    term_addition_version=2.1,
                    term_type="PT",
                    llt_currency=None,
                    action="A",
                ),
                Row(
                    term_code=200004,
                    term_name="17 ketosteroids urine decreased",
                    term_addition_version=4.1,
                    term_type="PT",
                    llt_currency=None,
                    action="U",
                ),
                Row(
                    term_code=200005,
                    term_name="cenas",
                    term_addition_version=2.1,
                    term_type="PT",
                    llt_currency=None,
                    action="A",
                ),
                Row(
                    term_code=200005,
                    term_name="cenas",
                    term_addition_version=2.1,
                    term_type="PT",
                    llt_currency=None,
                    action="D",
                ),
                Row(
                    term_code=200006,
                    term_name="doi doi",
                    term_addition_version=11.1,
                    term_type="PT",
                    llt_currency=None,
                    action="A",
                ),
                Row(
                    term_code=100006,
                    term_name="doi doi",
                    term_addition_version=11.1,
                    term_type="LLT",
                    llt_currency="Y",
                    action="A",
                ),
                Row(
                    term_code=100006,
                    term_name="doi doi",
                    term_addition_version=11.1,
                    term_type="PT",
                    llt_currency=None,
                    action="D",
                ),
            ]
        ),
        "meddra_release.asc": spark.createDataFrame([Row(version="25.1", language="English")]),
        "llt.asc": spark.createDataFrame(
            [
                Row(
                    llt_code=100006,
                    pt_code=200007,
                )
            ]
        ),
    }


@pytest.fixture
def pt_mappings(spark) -> Dict[str, DataFrame]:
    return {
        "pt_nnmq": spark.createDataFrame(
            [
                Row(pt_code=200001, nnmq_code=11),
                Row(pt_code=200002, nnmq_code=12),
                Row(pt_code=200007, nnmq_code=13),
            ]
        ),
        "pt_smq": spark.createDataFrame(
            [
                Row(pt_code=200002, smq_code=2.0),
                Row(pt_code=200002, smq_code=3.0),
                Row(pt_code=200007, smq_code=1.0),
            ]
        ),
        "pt_nnmq_smq_agg": spark.createDataFrame(
            [
                Row(pt_code=200001, smq_codes=None, nnmq_codes=[11]),
                Row(pt_code=200002, smq_codes=[2.0, 3.0], nnmq_codes=[12]),
                Row(pt_code=200007, smq_codes=[1.0], nnmq_codes=[13]),
            ]
        ),
    }


@pytest.fixture
def expected(spark) -> Dict[str, DataFrame]:
    return {
        "pt_nnmq": spark.createDataFrame(
            [
                Row(pt_code=200001, nnmq_code=11),
                Row(pt_code=200002, nnmq_code=12),
                Row(pt_code=200007, nnmq_code=13),
            ]
        ),
        "pt_smq": spark.createDataFrame(
            [
                Row(pt_code=200002, smq_code=2.0),
                Row(pt_code=200002, smq_code=3.0),
                Row(pt_code=200007, smq_code=1.0),
            ]
        ),
        "preferred_terms": spark.createDataFrame(
            [
                Row(
                    pt_code=200001,
                    pt="genetic disorders",
                    pt_soc_code=300001,
                    version="25.1",
                    actual_time=NOW,
                    smq_codes=None,
                    nnmq_codes=[11],
                ),
                Row(
                    pt_code=200002,
                    pt="11-beta-hydroxylase deficiency",
                    pt_soc_code=300002,
                    version="25.1",
                    actual_time=NOW,
                    smq_codes=[2.0, 3.0],
                    nnmq_codes=[12],
                ),
                Row(
                    pt_code=200003,
                    pt="17 ketosteroids urine",
                    pt_soc_code=300003,
                    version="25.1",
                    actual_time=NOW,
                    smq_codes=None,
                    nnmq_codes=None,
                ),
                Row(
                    pt_code=200004,
                    pt="17 ketosteroids urine decreased",
                    pt_soc_code=300004,
                    version="25.1",
                    actual_time=NOW,
                    smq_codes=None,
                    nnmq_codes=None,
                ),
                Row(
                    pt_code=200007,
                    pt="cenas que doem",
                    pt_soc_code=300005,
                    version="25.1",
                    actual_time=NOW,
                    smq_codes=[1.0],
                    nnmq_codes=[13],
                ),
                Row(
                    pt_code=200004,
                    pt="17 ketosteroids urine insufficient",
                    pt_soc_code=300004,
                    version="25.1",
                    actual_time=NOW,
                    smq_codes=None,
                    nnmq_codes=None,
                ),
            ]
        ),
        "llt_former_pt": spark.createDataFrame(
            [
                Row(
                    llt_code=100006,
                    llt_name="doi doi",
                    pt_code=200007,
                    pt_soc_code=300007,
                    smq_codes=[1.0],
                    nnmq_codes=[13],
                ),
            ]
        ),
    }


def test_etl(
    mocker,
    monkeypatch,
    medra_files: Dict[str, DataFrame],
    pt_mappings: Dict[str, DataFrame],
    expected: Dict[str, DataFrame],
):

    class MockMedDRAFiles:
        CONFIG_KEY_FILES_URL = "url"

        def __init__(self, config: Optional[Dict[str, Any]] = None):
            if self.CONFIG_KEY_FILES_URL not in config:
                raise SourceException(f"{self.CONFIG_KEY_FILES_URL} must be provided in config")

        def get(self, context: Optional[Dict[str, Any]] = None):
            return medra_files

        def list(self, context: Optional[Dict[str, Any]] = None):
            return [
                "/somewhere/pt.asc",
                "/somewhere/meddra_history_english.asc",
                "/somewhere/meddra_release.asc",
                "/somewhere/llt.asc",
            ]

    monkeypatch.setattr(mpp, "MedDRAFiles", MockMedDRAFiles)
    now: str = str(int(datetime.now(tz=timezone.utc).timestamp()))

    o = MedDRAPTProcessing(
        {
            "IsProcessingDue__updated_ts": now,
            "meddra_files_url": "dummy",
            "table_preferred_terms": "dummy",
            "datasets_url": "dummy",
            "meddra_mapping_src_url": "dummy",
        }
    )
    o.extract()

    class MockMedDRAPtSmqNNmqMappings:
        CONTEXT_KEY_MAPPING_SRC_URL = "mapping_src_url"
        OUTPUT_KEY_PT_SMQ = "pt_smq"
        OUTPUT_KEY_PT_NNMQ = "pt_nnmq"
        OUTPUT_KEY_PT_NNMQ_SMQ_AGG = "pt_nnmq_smq_agg"

        def process(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, DataFrame]:
            return pt_mappings

    monkeypatch.setattr(mpp, "MedDRAPtSmqNNmqMappings", MockMedDRAPtSmqNNmqMappings)
    o.transform()

    assert_frames_equality(o._data["pt_nnmq"].toPandas(), expected["pt_nnmq"].toPandas(), ["pt_code"])
    assert_frames_equality(o._data["pt_smq"].toPandas(), expected["pt_smq"].toPandas(), ["pt_code"])
    assert_frames_equality(
        o._data["preferred_terms"].drop("processing_time").toPandas(),
        expected["preferred_terms"].toPandas(),
        ["pt"],
    )
    assert_frames_equality(o._data["llt_former_pt"].toPandas(), expected["llt_former_pt"].toPandas(), ["pt_code"])

    mocked_store = mocker.patch("nn.gs.ss.meddra.etl.meddra_pt_processing.SparkDeltaStore")
    mocked_store_instance = mocked_store.return_value
    mocked_store_instance.save.return_value = None
    mocker.spy(mocked_store_instance, "save")

    mocked_catalog_store = mocker.patch("nn.gs.ss.meddra.etl.meddra_pt_processing.DatabricksCatalogStore")
    mocked_catalog_store_instance = mocked_catalog_store.return_value
    mocked_catalog_store_instance.save.return_value = None
    mocker.spy(mocked_catalog_store_instance, "save")

    o.load()
    assert mocked_catalog_store_instance.save.call_count == 1
    assert mocked_store_instance.save.call_count == 3
