import os
from typing import Any, List, Optional, Dict
import logging
from tgedr_dataops_abs.etl4gh import Etl4GH


logger = logging.getLogger(__name__)


class IsProcessingDue(Etl4GH):
    """
    Etl implementation class to find out if processing is due somehow.
    """

    _CONFIG_KEY_MEDDRA_PT_TABLE = "table_preferred_terms"
    _CONFIG_KEY_MEDDRA_LATEST_FILES_FOLDER_URL = "meddra_folder_url"
    _CONFIG_KEY_SMQ_NN_MAPPING_FILE_FOLDER_URL = "smq_nn_folder_url"

    def __init__(self, configuration: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(configuration=configuration)
        self._output: Dict[str, str] = {}

    @EtlDatabricks.inject_configuration
    def extract(self, table_preferred_terms: str) -> Any:
        """
        this method implements the `extract` step of the ETL workflow depicted in this class
        in this case we retrieve :
          - the latest actual time of the data currently in preferred_terms dataset
          - the modification time of the meddra files
          - the modification time of the SMQ-NNMQ mapping files

        if any of the modification times is different from the dataset actual time we should process data.
        """
        logger.info("[extract|in]")

        # find latest preferred terms processed release version
        pt_actual_time: int = self._find_preferred_terms_actual_time(table_preferred_terms)

        # find latest meddra files modification time
        meddra_files_modification_time: int = self._find_meddra_files_modification_ts()

        # find latest SMQ-NNMQ mapping file modification time
        smq_nn_mapping_files_modification_time: int = self._find_smq_nn_files_modification_ts()

        # if any of them (latest meddra files or latest SMQ-NNMQ mapping file)
        # differ from the preferred terms dataset `actual_time` then we need to reprocess
        decision: bool = (pt_actual_time < meddra_files_modification_time) or (
            pt_actual_time < smq_nn_mapping_files_modification_time
        )
        self._output["processing_due"] = "1" if decision == True else "0"
        self._output["updated_ts"] = str(max(meddra_files_modification_time, smq_nn_mapping_files_modification_time))

        logger.info("[extract|out]")

    def transform(self) -> Any:
        logger.info("[transform|in]")
        logger.info("[transform|out]")

    def load(self) -> bool:
        logger.info("[load|in]")
        logger.info(f"[load|out] {self._output}")
        return self._output

    def _find_meddra_files_modification_ts(self) -> int:
        logger.info("[_find_meddra_files_modification_ts|in]")
        result: int = 0

        url = os.path.join(self._configuration[self._CONFIG_KEY_MEDDRA_LATEST_FILES_FOLDER_URL], "meddra_release.asc")

        metadata: Dict[str, Any] = self._source.get_metadata(context={"source": url})
        result = int(metadata["LastModified"])

        logger.info(f"[_find_meddra_files_modification_ts|out] => {result}")
        return result

    def _find_smq_nn_files_modification_ts(self) -> int:
        logger.info("[_find_smq_nn_files_modification_ts|in]")
        result: int = 0

        folder_url: str = self._configuration[self._CONFIG_KEY_SMQ_NN_MAPPING_FILE_FOLDER_URL]
        files: List[str] = self._source.list(context={"source": folder_url})
        files.sort(reverse=True)
        url = files[0]
        metadata: Dict[str, Any] = self._source.get_metadata(context={"source": url})
        result = int(metadata["LastModified"])

        logger.info(f"[_find_smq_nn_files_modification_ts|out] => {result}")
        return result

    def _find_preferred_terms_actual_time(self, table_preferred_terms) -> int:
        logger.info("[_find_preferred_terms_actual_time|in]")
        result: int = 0
        # read the dataset
        try:
            result = self._store.get(table=table_preferred_terms).select("actual_time").take(1)[0].actual_time
        except NoStoreException as nse:
            # no dataset there
            logger.warning(f"[_find_preferred_terms_actual_time] => {nse}")

        logger.info(f"[_find_preferred_terms_actual_time|out] => {result}")
        return result
