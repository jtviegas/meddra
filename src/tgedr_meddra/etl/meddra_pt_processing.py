from typing import Any, List, Optional, Dict
import logging
import os


logger = logging.getLogger(__name__)


class MedDRAPTProcessing(EtlDatabricks):
    """
    Etl implementation class to process medra files and PT-NNMQ-SMQ mapping files and create a set of datasets:
    - `preferred_terms` codes and names with former names linked to the actual code and with also the relationship to SMQ and NNMQ
    - `pt-smq` mapping
    - `pt-nnmq` mapping
    - `llt_former_pt` - mapping of current llt's that were pt's before
    """

    __MEDDRA_FILES = ["meddra_history_english.asc", "pt.asc", "meddra_release.asc", "llt.asc"]
    __COLUMN_COMMENTS = {
        "preferred_terms": {
            "pt_code": "8-digit code to identify the Preferred Term",
            "pt": "Full name of the Preferred Term",
            "pt_soc_code": "The primary System Organ Class to which the Preferred Term is linked",
            "version": "medDRA version used to compute the current dataset",
            "nnmq_codes": "related NNMQ codes",
            "smq_codes": "related SMQ codes",
        },
        "llt_former_pt": {
            "llt_code": "8-digit code to identify the Lowest Level Term",
            "llt_name": "Full name of the Lowest Level Term",
            "pt_code": "8-digit code to identify the Preferred Term",
            "pt_soc_code": "The primary System Organ Class to which the Preferred Term is linked",
        },
    }
    __DATASET_PT_SMQ = "pt_smq"
    __DATASET_PT_NNMQ = "pt_nnmq"
    __DATASET_PT = "preferred_terms"
    __DATASET_LLT_FORMER_PT = "llt_former_pt"

    def __init__(self, configuration: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(configuration=configuration)
        self._medra_datasets: Dict[str, DataFrame] = None
        self._data: Dict[str, DataFrame] = None

    @EtlDatabricks.inject_configuration
    def extract(self, meddra_files_url: str) -> None:
        """
        this method implements the `extract` step of the ETL workflow depicted in this class
        in this case we extract the medra files (pt, llt, meddra_release and meddra_history_english)
        and load it into a pyspark DataFrame

        Parameters:
            meddra_files_url (str): where the files are located
        """
        logger.info(f"[extract|in] ({meddra_files_url})")
        medra_files_source: MedDRAFiles = MedDRAFiles({"url": meddra_files_url})
        medra_files: List[str] = [
            file for file in medra_files_source.list() if (os.path.basename(file) in self.__MEDDRA_FILES)
        ]
        self._medra_datasets: Dict[str, DataFrame] = medra_files_source.get({"files": medra_files})
        logger.info("[extract|out]")

    @EtlDatabricks.inject_configuration
    def transform(self, meddra_mapping_src_url: str, IsProcessingDue__updated_ts: str) -> None:
        """
        this method implements the `transform` step of the ETL workflow depicted in this class
        in this case we process the files into the different datasets

        Parameters:
            meddra_mapping_src_url (str): location of the SMW-NNMQ mapping file
            runtime (str): the current runtime
        """
        logger.info(f"[transform|in] ({meddra_mapping_src_url})")
        df_preferred_terms: DataFrame = MedDRAPrefferredTerms().process(
            {
                MedDRAPrefferredTerms.CONTEXT_KEY_DATASETS: self._medra_datasets,
                MedDRAPrefferredTerms.CONTEXT_KEY_RUNTIME: int(IsProcessingDue__updated_ts),
            }
        )

        mappings: Dict[str, DataFrame] = MedDRAPtSmqNNmqMappings().process(
            {
                MedDRAPtSmqNNmqMappings.CONTEXT_KEY_MAPPING_SRC_URL: meddra_mapping_src_url,
            }
        )
        self._data = {
            self.__DATASET_PT_SMQ: mappings[MedDRAPtSmqNNmqMappings.OUTPUT_KEY_PT_SMQ],
            self.__DATASET_PT_NNMQ: mappings[MedDRAPtSmqNNmqMappings.OUTPUT_KEY_PT_NNMQ],
        }
        df_llt_former_pt: DataFrame = MedDRALltFormerPt().process({"datasets": self._medra_datasets})
        map_output: Dict[str, DataFrame] = MapMedDRAPt().process(
            {
                MapMedDRAPt.CONTEXT_KEY_PT: df_preferred_terms,
                MapMedDRAPt.CONTEXT_KEY_PT_SMQ_NNMQ_AGG_MAPPING: mappings[
                    MedDRAPtSmqNNmqMappings.OUTPUT_KEY_PT_NNMQ_SMQ_AGG
                ],
                MapMedDRAPt.CONTEXT_KEY_LLT_FORMER_PT: df_llt_former_pt,
            }
        )
        self._data[self.__DATASET_PT] = map_output[MapMedDRAPt.OUTPUT_KEY_PT]
        self._data[self.__DATASET_LLT_FORMER_PT] = map_output[MapMedDRAPt.OUTPUT_KEY_LLT_FORMER_PT]
        logger.info("[transform|out]")

    @EtlDatabricks.inject_configuration
    def load(self, table_preferred_terms: str, datasets_url: str) -> None:
        """
        this method implements the `load` step of the ETL workflow depicted in this class
        in this case we save the datasets into datahub and run the crawler to update the hive metastore

        Parameters:
            table_preferred_terms (str): root location for the preferred terms table
            datasets_url (str): where to save the other datasets
        """
        logger.info(f"[load|in] ({table_preferred_terms}, {datasets_url})")
        if self._data is None:
            logger.info("[load|out] no data to process")
            return

        dbfsStore = SparkDeltaStore()
        catalogStore = DatabricksCatalogStore()
        for table, df in self._data.items():
            logger.info(f"[load] going to save dataset: {table}")
            column_descriptions = None if (table not in self.__COLUMN_COMMENTS) else self.__COLUMN_COMMENTS[table]

            if self.__DATASET_PT == table:
                catalogStore.save(df=df, table=table_preferred_terms, column_descriptions=column_descriptions)
            else:
                dbfsStore.save(df=df, key=os.path.join(datasets_url, table), column_descriptions=column_descriptions)

        logger.info("[load|out]")
