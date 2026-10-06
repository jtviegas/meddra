from typing import Any, Optional, Dict
import logging
from pyspark.sql import DataFrame
from ssds_pycommons.processor import Processor, ProcessorError

logger = logging.getLogger()


class MapMedDRAPt(Processor):
    """
    Processor implementation class responsible for joining the pt-SMQ and pt-NNMQ mappings to
    both `preferred_terms` and `llt_former_pt` datasets

    Parameters:
        config (Optional[Dict[str, Any]]): key-value map with configuration
            keys:
                preferred_terms - location of the files to be deflated
                pt_nnmq_smq_agg - location where to deflate the files into
                llt_former_pt   -
    """

    CONTEXT_KEY_PT = "preferred_terms"
    CONTEXT_KEY_PT_SMQ_NNMQ_AGG_MAPPING = "pt_nnmq_smq_agg"
    CONTEXT_KEY_LLT_FORMER_PT = "llt_former_pt"
    OUTPUT_KEY_PT = "preferred_terms"
    OUTPUT_KEY_LLT_FORMER_PT = "llt_former_pt"

    def process(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, DataFrame]:
        logger.info(f"[process|in] ({context})")

        result: Dict[str, DataFrame] = {}

        if not all(
            entry in context
            for entry in [self.CONTEXT_KEY_PT, self.CONTEXT_KEY_PT_SMQ_NNMQ_AGG_MAPPING, self.CONTEXT_KEY_LLT_FORMER_PT]
        ):
            raise ProcessorError(
                f"you must provide context for {self.CONTEXT_KEY_PT}, {self.CONTEXT_KEY_PT_SMQ_NNMQ_AGG_MAPPING} and {self.CONTEXT_KEY_LLT_FORMER_PT}"
            )

        # the preferred terms
        df_pt: DataFrame = context[self.CONTEXT_KEY_PT]
        # the smq and nnmq mappings
        df_pt_nnmq_smq_agg: DataFrame = context[self.CONTEXT_KEY_PT_SMQ_NNMQ_AGG_MAPPING]
        # the llt theat were pt
        df_llt_former_pt: DataFrame = context[self.CONTEXT_KEY_LLT_FORMER_PT]

        df_pt_with_mappings = df_pt.join(df_pt_nnmq_smq_agg, on="pt_code", how="left_outer")
        result[self.OUTPUT_KEY_PT] = df_pt_with_mappings

        df_llt_former_pt_with_mappings = df_llt_former_pt.join(df_pt_nnmq_smq_agg, on="pt_code", how="left_outer")
        result[self.OUTPUT_KEY_LLT_FORMER_PT] = df_llt_former_pt_with_mappings

        logger.info(f"[process|out] => {result}")
        return result
