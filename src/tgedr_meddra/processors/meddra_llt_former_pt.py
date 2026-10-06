from typing import Any, Optional, Dict
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ssds_pycommons.processor import Processor, ProcessorError


logger = logging.getLogger()


class MedDRALltFormerPt(Processor):
    CONTEXT_KEY_DATASETS = "datasets"

    def process(self, context: Optional[Dict[str, Any]] = None) -> DataFrame:
        logger.info(f"[process|in] ({context})")

        if self.CONTEXT_KEY_DATASETS not in context:
            raise ProcessorError(f"you must provide context for {self.CONTEXT_KEY_DATASETS}")

        datasets: Dict[str, DataFrame] = context[self.CONTEXT_KEY_DATASETS]
        df_hist: DataFrame = datasets["meddra_history_english.asc"]
        df_llt: DataFrame = datasets["llt.asc"]
        df_pt: DataFrame = datasets["pt.asc"]

        # ["term_type", "action", "term_code", "term_addition_version"]
        df_deleted_pts = self._get_deleted_pts(df_hist).drop("term_code")
        # Row(term_code=200005, term_name="cenas", term_addition_version=12.1,
        #     term_type="PT", llt_currency=None, action="D", )
        df_current_llts = self._get_current_llts(df_hist)
        # [Row(term_code=100006, term_name='doi doi')]

        # Row(term_code=100006, term_name="doi doi", term_addition_version=12.1,
        #     term_type="PT", llt_currency=None, action="D", )

        # Row(term_code=100006, term_name="doi doi", term_addition_version=12.1,
        #     term_type="PT", llt_currency=None, action="D", )

        result = (
            df_current_llts.join(df_deleted_pts, on=["term_name"])
            .withColumnRenamed("term_code", "llt_code")
            .withColumnRenamed("term_name", "llt_name")
            .join(df_llt.select("llt_code", "pt_code"), on="llt_code")
            .join(df_pt.select("pt_code", "pt_soc_code"), on="pt_code")
        )
        # Row(llt_code=100006, llt_name="doi doi", pt_code=200007, pt_soc_code=300007),

        logger.info(f"[process|out] => {result}")
        return result

    def _get_deleted_pts(self, df_hist: DataFrame) -> DataFrame:
        logger.info(f"[_get_deleted_pts|in] ({df_hist})")

        df_all_pt_deletes = (
            df_hist.select("term_type", "action", "term_code", "term_addition_version", "term_name")
            .filter((F.col("term_type") == "PT") & (F.col("action") == "D"))
            .groupBy("term_code", "term_name")
            .agg(F.max("term_addition_version").alias("deletion_version"))
        )
        df_all_pt_upserts = (
            df_hist.select("term_type", "action", "term_code", "term_addition_version", "term_name")
            .filter((F.col("term_type") == "PT") & (F.col("action").isin(["A", "U"])))
            .groupBy("term_code", "term_name")
            .agg(F.max("term_addition_version").alias("upsert_version"))
        )

        result = (
            df_all_pt_deletes.join(df_all_pt_upserts, on=["term_code", "term_name"], how="left")
            .fillna({"upsert_version": 0.0})
            .filter(F.col("deletion_version") > F.col("upsert_version"))
            .drop("deletion_version", "upsert_version")
        )

        logger.info(f"[_get_deleted_pts|out] => {result}")
        return result

    def _get_current_llts(self, df_hist: DataFrame) -> DataFrame:
        logger.info(f"[_get_current_llts|in] ({df_hist})")

        df_all_llt_deletes = (
            df_hist.select("term_type", "action", "term_code", "term_addition_version", "term_name")
            .filter((F.col("term_type") == "LLT") & (F.col("action") == "D"))
            .groupBy("term_code", "term_name")
            .agg(F.max("term_addition_version").alias("deletion_version"))
        )
        df_all_llt_upserts = (
            df_hist.select("term_type", "action", "term_code", "term_addition_version", "term_name")
            .filter((F.col("term_type") == "LLT") & (F.col("action").isin(["A", "U"])))
            .groupBy("term_code", "term_name")
            .agg(F.max("term_addition_version").alias("upsert_version"))
        )

        result = (
            df_all_llt_upserts.join(df_all_llt_deletes, on=["term_code", "term_name"], how="left")
            .fillna({"deletion_version": 0.0})
            .filter(F.col("upsert_version") > F.col("deletion_version"))
            .drop("deletion_version", "upsert_version")
        )

        logger.info(f"[_get_current_llts|out] => {result}")
        return result
