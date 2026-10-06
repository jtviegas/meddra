import os
from test.conftest import assert_frames_equality
from typing import Dict, List

import pytest
from pyspark.sql import DataFrame, Row

from nn.gs.ss.meddra.sources.meddra_files import MedDRAFiles


@pytest.fixture
def expected(spark) -> DataFrame:
    return {
        "pt.asc": spark.createDataFrame(
            [
                Row(
                    pt_code=10000002,
                    pt_name="11-beta-hydroxylase deficiency",
                    null_field=None,
                    pt_soc_code=10010331,
                    pt_whoart_code=None,
                    pt_harts_code=None,
                    pt_costart_sym=None,
                    pt_icd9_code=None,
                    pt_icd9cm_code=None,
                    pt_icd10_code=None,
                    pt_jart_code=None,
                ),
                Row(
                    pt_code=10000005,
                    pt_name="17 ketosteroids urine",
                    null_field=None,
                    pt_soc_code=10022891,
                    pt_whoart_code=None,
                    pt_harts_code=None,
                    pt_costart_sym=None,
                    pt_icd9_code=None,
                    pt_icd9cm_code=None,
                    pt_icd10_code=None,
                    pt_jart_code=None,
                ),
                Row(
                    pt_code=10000007,
                    pt_name="17 ketosteroids urine decreased",
                    null_field=None,
                    pt_soc_code=10022891,
                    pt_whoart_code=None,
                    pt_harts_code=None,
                    pt_costart_sym=None,
                    pt_icd9_code=None,
                    pt_icd9cm_code=None,
                    pt_icd10_code=None,
                    pt_jart_code=None,
                ),
            ],
            schema=MedDRAFiles._SCHEMAS["pt.asc"],
        ),
        "meddra_history_english.asc": spark.createDataFrame(
            [
                Row(
                    term_code=10005329,
                    term_name="Blood and lymphatic system disorders",
                    term_addition_version=2.1,
                    term_type="SOC",
                    llt_currency=None,
                    action="A",
                ),
                Row(
                    term_code=10007541,
                    term_name="Cardiac disorders",
                    term_addition_version=2.1,
                    term_type="SOC",
                    llt_currency=None,
                    action="A",
                ),
                Row(
                    term_code=10010331,
                    term_name="Congenital and familial/genetic disorders",
                    term_addition_version=2.1,
                    term_type="SOC",
                    llt_currency=None,
                    action="A",
                ),
            ],
            schema=MedDRAFiles._SCHEMAS["meddra_history_english.asc"],
        ),
        "meddra_release.asc": spark.createDataFrame(
            [
                Row(
                    version="25.1",
                    language="English",
                )
            ],
            schema=MedDRAFiles._SCHEMAS["meddra_release.asc"],
        ),
        "llt.asc": spark.createDataFrame(
            [
                Row(
                    llt_code=10000001,
                    llt_name='"Ventilation" pneumonitis',
                    pt_code=10081988,
                    llt_whoart_code=None,
                    llt_harts_code=None,
                    llt_costart_sym=None,
                    llt_icd9_code=None,
                    llt_icd9cm_code=None,
                    llt_icd10_code=None,
                    llt_currency="N",
                    llt_jart_code=None,
                    unnamed_1=None,
                ),
                Row(
                    llt_code=10000002,
                    llt_name="11-beta-hydroxylase deficiency",
                    pt_code=10000002,
                    llt_whoart_code=None,
                    llt_harts_code=None,
                    llt_costart_sym=None,
                    llt_icd9_code=None,
                    llt_icd9cm_code=None,
                    llt_icd10_code=None,
                    llt_currency="Y",
                    llt_jart_code=None,
                    unnamed_1=None,
                ),
                Row(
                    llt_code=10000003,
                    llt_name="11-oxysteroid activity incr",
                    pt_code=10033315,
                    llt_whoart_code=None,
                    llt_harts_code=None,
                    llt_costart_sym=None,
                    llt_icd9_code=None,
                    llt_icd9cm_code=None,
                    llt_icd10_code=None,
                    llt_currency="N",
                    llt_jart_code=None,
                    unnamed_1=None,
                ),
                Row(
                    llt_code=10000004,
                    llt_name="11-oxysteroid activity increased",
                    pt_code=10033315,
                    llt_whoart_code=None,
                    llt_harts_code=None,
                    llt_costart_sym=None,
                    llt_icd9_code=None,
                    llt_icd9cm_code=None,
                    llt_icd10_code=None,
                    llt_currency="Y",
                    llt_jart_code=None,
                    unnamed_1=None,
                ),
                Row(
                    llt_code=10000005,
                    llt_name="17 ketosteroids urine",
                    pt_code=10000005,
                    llt_whoart_code=None,
                    llt_harts_code=None,
                    llt_costart_sym=None,
                    llt_icd9_code=None,
                    llt_icd9cm_code=None,
                    llt_icd10_code=None,
                    llt_currency="Y",
                    llt_jart_code=None,
                    unnamed_1=None,
                ),
                Row(
                    llt_code=10000006,
                    llt_name="17 ketosteroids urine abnormal NOS",
                    pt_code=10061608,
                    llt_whoart_code=None,
                    llt_harts_code=None,
                    llt_costart_sym=None,
                    llt_icd9_code=None,
                    llt_icd9cm_code=None,
                    llt_icd10_code=None,
                    llt_currency="Y",
                    llt_jart_code=None,
                    unnamed_1=None,
                ),
                Row(
                    llt_code=10000007,
                    llt_name="17 ketosteroids urine decreased",
                    pt_code=10000007,
                    llt_whoart_code=None,
                    llt_harts_code=None,
                    llt_costart_sym=None,
                    llt_icd9_code=None,
                    llt_icd9cm_code=None,
                    llt_icd10_code=None,
                    llt_currency="Y",
                    llt_jart_code=None,
                    unnamed_1=None,
                ),
            ],
            schema=MedDRAFiles._SCHEMAS["llt.asc"],
        ),
    }


def test_source(resources_folder, expected):
    folder = os.path.join(resources_folder, "meddra")

    o = MedDRAFiles({"url": folder, "source_impl": "nn.gs.ss.dataops.source.local_fs_file_source.LocalFsFileSource"})

    actual: List[str] = o.list()
    actual.sort()

    assert actual == [
        os.path.join(folder, "llt.asc"),
        os.path.join(folder, "meddra_history_english.asc"),
        os.path.join(folder, "meddra_release.asc"),
        os.path.join(folder, "pt.asc"),
    ]

    actual: Dict[str, DataFrame] = o.get({"files": actual})

    assert_frames_equality((actual["pt.asc"]).toPandas(), (expected["pt.asc"]).toPandas(), ["pt_code"])
    assert_frames_equality(
        (actual["meddra_history_english.asc"]).toPandas(),
        (expected["meddra_history_english.asc"]).toPandas(),
        ["term_code"],
    )
    assert_frames_equality(
        (actual["meddra_release.asc"]).toPandas(),
        (expected["meddra_release.asc"]).toPandas(),
        ["version"],
    )
    assert_frames_equality(
        (actual["llt.asc"]).toPandas(),
        (expected["llt.asc"]).toPandas(),
        ["llt_code"],
    )
