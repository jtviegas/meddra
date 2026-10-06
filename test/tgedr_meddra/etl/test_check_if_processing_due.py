import time
from datetime import datetime, timezone
from typing import Dict

from pyspark.sql import Row

import nn.gs.ss.meddra.etl.check_if_processing_due as cipd
from nn.gs.ss.meddra.etl.check_if_processing_due import IsProcessingDue

ACTUAL_TIME: int = 1717156673
VERSION = "23.4"


def getDataset(spark):
    return spark.createDataFrame(
        [
            Row(
                pt_code=10000002,
                pt="11-beta-hydroxylase deficiency",
                pt_soc_code=10010331,
                version=VERSION,
                actual_time=ACTUAL_TIME,
            ),
            Row(
                pt_code=10000002,
                pt="11-beta-hydroxylase deficiency a",
                pt_soc_code=10010331,
                version=VERSION,
                actual_time=ACTUAL_TIME,
            ),
        ]
    )


def test_find_medra_files_ts(monkeypatch, environment_mock):
    now: int = int(datetime.now(tz=timezone.utc).timestamp())
    time.sleep(1)

    class MockCatalogFileSource:
        def get_metadata(self, context):
            return {"LastModified": now + 1}

    monkeypatch.setattr(cipd, "CatalogFileSource", MockCatalogFileSource)
    o = IsProcessingDue(
        configuration={"table_preferred_terms": "dummy", "meddra_folder_url": "dummy", "smq_nn_folder_url": "dummy"}
    )

    assert o._find_meddra_files_modification_ts() > now


def test_find_smq_nn_files_ts(monkeypatch, environment_mock):

    now: int = int(datetime.now(tz=timezone.utc).timestamp())

    class MockCatalogFileSource:
        def get_metadata(self, context):
            return {"LastModified": now + 1}

        def list(self, context):
            return ["dummy1", "dummy2"]

    time.sleep(1)
    monkeypatch.setattr(cipd, "CatalogFileSource", MockCatalogFileSource)
    o = IsProcessingDue(
        configuration={"table_preferred_terms": "dummy", "meddra_folder_url": "dummy", "smq_nn_folder_url": "dummy"}
    )
    assert o._find_smq_nn_files_modification_ts() > now


def test_find_preferred_terms_actual_time(environment_mock, spark, monkeypatch):

    class MockDatabricksCatalogStore:
        def get(self, table):
            return getDataset(spark)

    monkeypatch.setattr(cipd, "DatabricksCatalogStore", MockDatabricksCatalogStore)
    # do the actual test
    o = IsProcessingDue(
        configuration={"table_preferred_terms": "dummy", "meddra_folder_url": "dummy", "smq_nn_folder_url": "dummy"}
    )
    assert ACTUAL_TIME == o._find_preferred_terms_actual_time("dummy")


def test_etl(spark, monkeypatch):
    now: int = int(datetime.now(tz=timezone.utc).timestamp())
    time.sleep(1)

    class MockDatabricksCatalogStore:
        def get(self, table):
            return getDataset(spark)

    class MockCatalogFileSource:
        def get_metadata(self, context):
            return {"LastModified": now + 1}

        def list(self, context):
            return ["dummy1", "dummy2"]

    monkeypatch.setattr(cipd, "DatabricksCatalogStore", MockDatabricksCatalogStore)
    monkeypatch.setattr(cipd, "CatalogFileSource", MockCatalogFileSource)

    # do the actual test
    o = IsProcessingDue(
        configuration={
            "table_preferred_terms": "dataset_url",
            "meddra_folder_url": "medra_files_url",
            "smq_nn_folder_url": "smq_mapping_url",
        }
    )
    actual: Dict[str, str] = o.run()
    assert now < int(actual["updated_ts"])
    assert "1" == actual["processing_due"]
