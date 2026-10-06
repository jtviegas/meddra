import os
from typing import Any, List, Optional, Dict
import logging
from pyspark.sql import DataFrame
import pyspark.sql.types as T

from nn.gs.ss.dataops.source.source import Source, SourceException
from nn.gs.ss.dataops.commons.utils_spark import UtilsSpark
from ssds_pycommons.utils_reflection import UtilsReflection

logger = logging.getLogger()


class MedDRAFiles(Source):
    CONFIG_KEY_FILES_URL = "url"
    CONTEXT_KEY_FILES = "files"

    CONFIG_KEY_SOURCE_IMPL = "source_impl"

    _SCHEMAS = {
        "pt.asc": T.StructType(
            [
                T.StructField("pt_code", T.LongType(), False),
                T.StructField("pt_name", T.StringType(), False),
                T.StructField("null_field", T.StringType(), True),
                T.StructField("pt_soc_code", T.LongType(), True),
                T.StructField("pt_whoart_code", T.StringType(), True),
                T.StructField("pt_harts_code", T.LongType(), True),
                T.StructField("pt_costart_sym", T.StringType(), True),
                T.StructField("pt_icd9_code", T.StringType(), True),
                T.StructField("pt_icd9cm_code", T.StringType(), True),
                T.StructField("pt_icd10_code", T.StringType(), True),
                T.StructField("pt_jart_code", T.StringType(), True),
            ]
        ),
        "meddra_history_english.asc": T.StructType(
            [
                T.StructField("term_code", T.LongType(), False),
                T.StructField("term_name", T.StringType(), False),
                T.StructField("term_addition_version", T.StringType(), False),
                T.StructField("term_type", T.StringType(), False),
                T.StructField("llt_currency", T.StringType(), True),
                T.StructField("action", T.StringType(), False),
            ]
        ),
        "meddra_release.asc": T.StructType(
            [T.StructField("version", T.StringType(), False), T.StructField("language", T.StringType(), False)]
        ),
        "llt.asc": T.StructType(
            [
                T.StructField("llt_code", T.LongType(), False),
                T.StructField("llt_name", T.StringType(), False),
                T.StructField("pt_code", T.LongType(), False),
                T.StructField("llt_whoart_code", T.DoubleType(), True),
                T.StructField("llt_harts_code", T.DoubleType(), True),
                T.StructField("llt_costart_sym", T.DoubleType(), True),
                T.StructField("llt_icd9_code", T.DoubleType(), True),
                T.StructField("llt_icd9cm_code", T.DoubleType(), True),
                T.StructField("llt_icd10_code", T.DoubleType(), True),
                T.StructField("llt_currency", T.StringType(), False),
                T.StructField("llt_jart_code", T.DoubleType(), True),
                T.StructField("unnamed_1", T.DoubleType(), True),
            ]
        ),
    }

    _CSV_DELIMITER = "$"

    __DEFAULT_SOURCE_IMPL = "nn.gs.ss.dataops.source.catalog_file_source.CatalogFileSource"

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config=config)
        if self.CONFIG_KEY_FILES_URL not in self._config:
            raise SourceException(f"{self.CONFIG_KEY_FILES_URL} must be provided in config")

        if self.CONFIG_KEY_SOURCE_IMPL not in self._config:
            source_class = UtilsReflection.load_class(self.__DEFAULT_SOURCE_IMPL, Source)
        else:
            source_class = UtilsReflection.load_class(config[self.CONFIG_KEY_SOURCE_IMPL], Source)

        self._source: Source = source_class()
        self._files_url: str = self._config[self.CONFIG_KEY_FILES_URL]

    def list(self, context: Optional[Dict[str, Any]] = None) -> List[str]:
        logger.info(f"[list|in] ({context})")
        result: List[str] = self._source.list({"source": self._files_url, "file_suffix": ".asc"})
        logger.info(f"[list|out] => {result}")
        return result

    def get(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, DataFrame]:
        logger.info(f"[get|in] ({context})")

        if self.CONTEXT_KEY_FILES not in context:
            raise SourceException(f"{self.CONTEXT_KEY_FILES} must be provided in context")

        files: List[str] = context[self.CONTEXT_KEY_FILES]

        result: Dict[str, DataFrame] = {}
        for file in files:
            filename = os.path.basename(file)
            result[filename] = self._read_file(file)

        logger.info(f"[get|out] => {result}")
        return result

    def _read_file(self, file_url) -> DataFrame:
        logger.info(f"[_read_file|in] ({file_url})")

        filename = os.path.basename(file_url)
        if filename in self._SCHEMAS:
            schema = self._SCHEMAS[filename]
            result = UtilsSpark.get_spark_session().read.csv(
                file_url, header=False, schema=schema, sep=self._CSV_DELIMITER
            )
        else:
            result = UtilsSpark.get_spark_session().read.csv(file_url, header=False, sep=self._CSV_DELIMITER)

        logger.info(f"[_read_file|out] => {result}")
        return result
