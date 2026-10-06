from typing import Any, Optional, Dict
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import datetime, timezone

from ssds_pycommons.processor import Processor, ProcessorError


logger = logging.getLogger()


class MedDRAPrefferredTerms(Processor):
    CONTEXT_KEY_DATASETS = "datasets"
    CONTEXT_KEY_RUNTIME = "runtime"

    def process(self, context: Optional[Dict[str, Any]] = None) -> DataFrame:
        logger.info(f"[process|in] ({context})")

        if (self.CONTEXT_KEY_DATASETS not in context) or (self.CONTEXT_KEY_RUNTIME not in context):
            raise ProcessorError(
                f"you must provide context for {self.CONTEXT_KEY_DATASETS} and {self.CONTEXT_KEY_RUNTIME}"
            )

        runtime: int = context[self.CONTEXT_KEY_RUNTIME]
        datasets: Dict[str, DataFrame] = context[self.CONTEXT_KEY_DATASETS]

        medra_version: str = (datasets["meddra_release.asc"].collect()[0]).version
        now: int = int(datetime.now(tz=timezone.utc).timestamp())

        df_pt: DataFrame = (
            (datasets["pt.asc"])
            .withColumnRenamed("pt_name", "pt")
            .withColumn("pt", F.lower(F.col("pt")))
            .select("pt_code", "pt", "pt_soc_code")
        )
        # get the updated and the added preferred terms
        df_pt_hist: DataFrame = (
            (datasets["meddra_history_english.asc"])
            .na.fill(value="N", subset=["llt_currency"])
            .filter(F.col("term_type") == "PT")
            .withColumn("term_name", F.lower(F.col("term_name")))
            .withColumnRenamed("term_name", "pt")
            .withColumnRenamed("term_code", "pt_code")
            # TODO: or shall we take all ?
            .filter(F.col("action").isin(["U", "A", "D"]))
            .select("pt_code", "pt")
            .distinct()
        )

        # join with pt_code
        result: DataFrame = (
            df_pt.join(df_pt_hist.withColumnRenamed("pt", "pt_hist"), on="pt_code", how="left_outer")
            .withColumn(
                "preferred_term",
                F.when(F.col("pt_hist").isNull(), F.col("pt")).otherwise(
                    F.when(F.col("pt") == F.col("pt_hist"), F.col("pt")).otherwise(F.col("pt_hist"))
                ),
            )
            .drop("pt", "pt_hist")
            .withColumnRenamed("preferred_term", "pt")
        ).distinct()

        result = (
            result.withColumn("version", F.lit(medra_version))
            .withColumn("processing_time", F.lit(now))
            .withColumn("actual_time", F.lit(runtime))
        )

        logger.info(f"[process|out] => {result}")
        return result
