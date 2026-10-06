from datetime import datetime, timezone
from test.conftest import assert_frames_equality
from typing import Dict

import pytest
from numpy import nan
from pyspark.sql import DataFrame, Row

from nn.gs.ss.meddra.processors.meddra_preferred_terms import MedDRAPrefferredTerms

NOW: int = int(datetime.now(tz=timezone.utc).timestamp())


@pytest.fixture
def datasets(spark) -> Dict[str, DataFrame]:
    return {
        "pt.asc": spark.createDataFrame(
            [
                Row(
                    pt_code=10000002,
                    pt_name="11-beta-hydroxylase deficiency",
                    null_field=nan,
                    pt_soc_code=10010331,
                    pt_whoart_code=nan,
                    pt_harts_code=nan,
                    pt_costart_sym=nan,
                    pt_icd9_code=nan,
                    pt_icd9cm_code=nan,
                    pt_icd10_code=nan,
                    pt_jart_code=nan,
                ),
                Row(
                    pt_code=10000005,
                    pt_name="17 ketosteroids urine",
                    null_field=nan,
                    pt_soc_code=10022891,
                    pt_whoart_code=nan,
                    pt_harts_code=nan,
                    pt_costart_sym=nan,
                    pt_icd9_code=nan,
                    pt_icd9cm_code=nan,
                    pt_icd10_code=nan,
                    pt_jart_code=nan,
                ),
                Row(
                    pt_code=10000007,
                    pt_name="17 ketosteroids urine decreased",
                    null_field=nan,
                    pt_soc_code=10022891,
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
                    term_code=10000002,
                    term_name="11-beta-hydroxylase deficiency A",
                    term_addition_version=2.1,
                    term_type="PT",
                    llt_currency=nan,
                    action="A",
                ),
                Row(
                    term_code=10000002,
                    term_name="11-beta-hydroxylase deficiency",
                    term_addition_version=2.1,
                    term_type="PT",
                    llt_currency=nan,
                    action="U",
                ),
                Row(
                    term_code=10000005,
                    term_name="17 ketosteroids urine",
                    term_addition_version=2.1,
                    term_type="PT",
                    llt_currency=nan,
                    action="A",
                ),
                Row(
                    term_code=10010331,
                    term_name="Congenital and familial/genetic disorders",
                    term_addition_version=2.1,
                    term_type="SOC",
                    llt_currency=nan,
                    action="D",
                ),
            ]
        ),
        "meddra_release.asc": spark.createDataFrame([Row(version="25.1", language="English")]),
    }


@pytest.fixture
def expected(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                pt_code=10000002,
                pt="11-beta-hydroxylase deficiency",
                pt_soc_code=10010331,
                version="25.1",
                actual_time=NOW,
            ),
            Row(
                pt_code=10000002,
                pt="11-beta-hydroxylase deficiency a",
                pt_soc_code=10010331,
                version="25.1",
                actual_time=NOW,
            ),
            Row(pt_code=10000005, pt="17 ketosteroids urine", pt_soc_code=10022891, version="25.1", actual_time=NOW),
            Row(
                pt_code=10000007,
                pt="17 ketosteroids urine decreased",
                pt_soc_code=10022891,
                version="25.1",
                actual_time=NOW,
            ),
        ]
    )


def test_process(resources_folder, datasets, expected):
    o: MedDRAPrefferredTerms = MedDRAPrefferredTerms()
    actual = o.process({"datasets": datasets, "runtime": NOW})

    assert_frames_equality(actual.drop("processing_time").toPandas(), expected.toPandas(), ["pt"])
