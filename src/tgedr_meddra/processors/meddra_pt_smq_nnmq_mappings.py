from typing import Any, List, Optional, Dict
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pyspark.sql.types as T
import pandas as pd

from ssds_pycommons.processor import Processor, ProcessorError
from nn.gs.ss.dataops.commons.utils_spark import UtilsSpark
from nn.gs.ss.dataops.source.catalog_file_source import CatalogFileSource
from nn.gs.ss.dataops.source.pd_df_catalog_source import PdDfCatalogSource


logger = logging.getLogger()


class MedDRAPtSmqNNmqMappings(Processor):
    CONTEXT_KEY_MAPPING_SRC_URL = "mapping_src_url"
    OUTPUT_KEY_PT_SMQ = "pt_smq"
    OUTPUT_KEY_PT_NNMQ = "pt_nnmq"
    OUTPUT_KEY_PT_NNMQ_SMQ_AGG = "pt_nnmq_smq_agg"

    __SRC_MAPPING_SCHEMA = T.StructType(
        [
            T.StructField("NNMQ_SEQ_NO", T.LongType(), True),
            T.StructField("NNMQ_PT_NAME", T.StringType(), True),
            T.StructField("SHOW_CATEGORY", T.StringType(), True),
            T.StructField("CATEGORY_SERIAL_NUMBER", T.LongType(), True),
            T.StructField("MAPPING_CATEGORY", T.StringType(), True),
            T.StructField("SHOW_FILTER", T.StringType(), True),
            T.StructField("TERM_SCOPE_TEXT", T.StringType(), True),
            T.StructField("TERM_SCOPE", T.LongType(), True),
            T.StructField("TERM_CATEGORY", T.StringType(), True),
            T.StructField("SMQ_ALGORITHM", T.StringType(), True),
            T.StructField("smq_name", T.StringType(), True),
            T.StructField("SMQ_CODE", T.DoubleType(), True),
            T.StructField("hlgt_name", T.StringType(), True),
            T.StructField("hlt_name", T.StringType(), True),
            T.StructField("ART_CODE", T.LongType(), True),
            T.StructField("NNMQ_PT_CODE", T.LongType(), True),
            T.StructField("MEDDRA_VER", T.DoubleType(), True),
        ]
    )

    def _find_latest_mapping_file_url(self, url: str) -> str:
        logger.info(f"[_find_latest_mapping_file_url|in] ({url})")
        result: str = None

        src: CatalogFileSource = CatalogFileSource()
        files: List[str] = src.list(context={"source": url})
        files.sort(reverse=True)
        result = files[0]

        logger.info(f"[_find_latest_mapping_file_url|out] => {result}")
        return result

    def _read_mappings(self, url: str) -> DataFrame:
        logger.info(f"[_read_mappings|in] ({url})")
        result: DataFrame = None

        source = PdDfCatalogSource()
        pd_df: pd.DataFrame = source.get({"file": url, "file_format": "xlsx"})
        result: DataFrame = (
            UtilsSpark.get_spark_session()
            .createDataFrame(pd_df, schema=self.__SRC_MAPPING_SCHEMA)
            .select("CATEGORY_SERIAL_NUMBER", "SMQ_CODE", "NNMQ_PT_CODE")
        )

        logger.info(f"[_read_mappings|out] => {result}")
        return result

    def process(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, DataFrame]:
        logger.info(f"[process|in] ({context})")

        result: Dict[str, DataFrame] = {}

        if self.CONTEXT_KEY_MAPPING_SRC_URL not in context:
            raise ProcessorError(f"you must provide context for {self.CONTEXT_KEY_MAPPING_SRC_URL}")

        mapping_src_url: str = context[self.CONTEXT_KEY_MAPPING_SRC_URL]
        mappping_file_url: str = self._find_latest_mapping_file_url(url=mapping_src_url)
        df_mappings: DataFrame = self._read_mappings(url=mappping_file_url)

        df_pt = (
            df_mappings.select("NNMQ_PT_CODE")
            .distinct()
            .orderBy(F.col("NNMQ_PT_CODE"))
            .withColumnRenamed("NNMQ_PT_CODE", "pt_code")
        )
        df_pt_nnmq = (
            df_mappings.select("NNMQ_PT_CODE", "CATEGORY_SERIAL_NUMBER")
            .filter((~F.isnan("CATEGORY_SERIAL_NUMBER")) & (0 < F.col("CATEGORY_SERIAL_NUMBER")))
            .distinct()
            .orderBy(F.col("NNMQ_PT_CODE"))
            .withColumnRenamed("NNMQ_PT_CODE", "pt_code")
            .withColumnRenamed("CATEGORY_SERIAL_NUMBER", "nnmq_code")
        )
        df_pt_smq = (
            df_mappings.select("NNMQ_PT_CODE", "SMQ_CODE")
            .filter(~F.isnan("SMQ_CODE"))
            .distinct()
            .orderBy(F.col("NNMQ_PT_CODE"))
            .withColumnRenamed("NNMQ_PT_CODE", "pt_code")
            .withColumnRenamed("SMQ_CODE", "smq_code")
            .withColumn("smq_code", F.col("smq_code").cast(T.LongType()))
        )

        df_pt_nnmq_agg = df_pt_nnmq.groupBy("pt_code").agg(F.collect_set("nnmq_code").alias("nnmq_codes"))
        df_pt_smq_agg = df_pt_smq.groupBy("pt_code").agg(F.collect_set("smq_code").alias("smq_codes"))

        df_pt_nnmq_smq_agg = df_pt.join(df_pt_nnmq_agg, on="pt_code", how="left_outer").join(
            df_pt_smq_agg, on="pt_code", how="left_outer"
        )

        result[self.OUTPUT_KEY_PT_NNMQ_SMQ_AGG] = df_pt_nnmq_smq_agg
        result[self.OUTPUT_KEY_PT_SMQ] = df_pt_smq
        result[self.OUTPUT_KEY_PT_NNMQ] = df_pt_nnmq

        logger.info(f"[process|out] => {result}")
        return result
