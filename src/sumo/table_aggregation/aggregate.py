"""Contains classes for aggregation of tables"""
import warnings
import asyncio
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from sumo.wrapper import SumoClient
import sumo.table_aggregation.utilities as ut


class TableAggregator:

    """Class for aggregating tables"""

    def __init__(
        self,
        case_identifier: str,
        name: str,
        tag: str,
        iteration: str,
        token: str = None,
        **kwargs
    ):
        """Read the data to be aggregated
        args:
        case_identifier (str): name of sumo case
        name (str): name of tables to aggregate
        tag (str): name of tag for table
        token (str): authentication token
        """
        self._logger = ut.init_logging(__file__ + ".TableAggregator")
        sumo_env = kwargs.get("sumo_env", "prod")
        self._sumo = SumoClient(sumo_env, token)
        self._case_identifier = ut.return_uuid(self._sumo, case_identifier)
        self._name = name
        self.loop = asyncio.get_event_loop()
        self._iteration = iteration
        # try:
        (
            self._parent_id,
            self._object_ids,
            self._meta,
            self._table_index,
        ) = ut.query_for_table(
            self.sumo, self._case_identifier, self._name, tag, self._iteration, **kwargs
        )

    @property
    def name(self) -> str:
        """Return _name attribute

        Returns:
            str: name of table
        """
        return self._name

    @property
    def case_identifier(self) -> str:
        """Return _case_name attribute

        Returns:
            str: name of table
        """
        return self._case_identifier

    @property
    def parent_id(self) -> str:
        """Return _parent_id attribute"""
        return self._parent_id

    @property
    def table_index(self):
        """Return attribute _table_index

        Returns:
            string: the table index
        """
        return self._table_index

    @property
    def sumo(self) -> SumoClient:
        """return the _sumo_attribute"""
        return self._sumo

    @property
    def object_ids(self) -> tuple:
        """Return the _object_ids attribute"""
        return self._object_ids

    @property
    def iteration(self) -> str:
        """Return the _iteration attribute"""
        return self._iteration

    @property
    def base_meta(self) -> dict:
        """Return _meta attribute"""
        return self._meta

    @property
    def columns(self):
        """Return _meta["data"]["spec"]["columns"] split into batches of 1000

        Returns:
            list: the columns of the table set provided
        """
        largest_size = 1000
        segments = ut.split_list(
            self.base_meta["data"]["spec"]["columns"], largest_size
        )
        segs_w_table_index = []
        for segment in segments:
            seg_set = set(segment)
            try:
                seg_set.update(self.table_index)
            except TypeError:
                self._logger.warning("Cannot add index, is %s", self.table_index)
            segs_w_table_index.append(tuple(seg_set))
        return tuple(segs_w_table_index)

    @property
    def aggregated(self) -> pd.DataFrame:
        """Return the _aggregated attribute"""

        return self._aggregated

    @aggregated.setter
    def aggregated(self, aggregated):
        """Set the _aggregated attribute

        Args:
            aggregated (pa.Table): aggregated results
        """
        self._aggregated = aggregated
        # self._logger.info("Aggregated results %s", aggregated)

    @ut.timethis("aggregation")
    @ut.memcount()
    def aggregate(self, columns):
        """Aggregate objects over tables per real stored in sumo"""
        self._logger.info("table_index for aggregation: %s", self.table_index)
        if (self.table_index is not None) and (len(self.table_index) > 0):
            # self.aggregated = None
            self.aggregated = self.loop.run_until_complete(
                ut.aggregate_arrow(
                    self.object_ids,
                    self.sumo,
                    columns,
                    self.loop,
                )
            )
        else:
            self.aggregated = None
            self._logger.warning(
                "No aggregation will be done, no table index",
            )

    @ut.timethis("upload")
    def upload(self):
        """Upload data to sumo"""
        if self.aggregated is not None:
            executor = ThreadPoolExecutor()
            self.loop.run_until_complete(
                ut.extract_and_upload(
                    self.sumo,
                    self.parent_id,
                    self.aggregated,
                    self.table_index,
                    self.base_meta,
                    self.loop,
                    executor,
                )
            )
        else:
            warnings.warn("No aggregation in place, so no upload will be done!!")

    def run(self):
        """Run aggregation and upload"""
        for list_seg in self.columns:
            self.aggregate(list_seg)
            self.upload()


class AggregationRunner:
    """Class for running all aggregations of tables for specific case"""

    def __init__(self, uuid: str, env: str = "prod") -> None:
        """Init of sumo env

        Args:
            uuid (str): the uuid of the case
            env (str, optional): name of the sumo environment for case, default prod
        """
        self._logger = ut.init_logging(__name__ + ".AggregationRunner")
        self._env = env
        self._uuid = uuid
        self._sumo = SumoClient(env)

    @property
    def uuid(self):
        """Return uuid of case

        Returns:
            str: uuid of case
        """
        return self._uuid

    @property
    def env(self):
        """Return environment of case

        Returns:
            str: sumo environment
        """
        return self._env

    def run(self) -> None:
        """Run all aggregation related to case"""

        iterations = ut.query_sumo_iterations(self._sumo, self.uuid)
        for iter_name in iterations:
            names_w_tags = ut.query_for_name_and_tags(self._sumo, self.uuid, iter_name)

            for name, tag_list in names_w_tags.items():
                self._logger.info("\nData.name: %s", name)
                for tag in tag_list:
                    self._logger.info("  data.tagname: %s", tag)
                    aggregator = TableAggregator(
                        self._uuid,
                        name,
                        tag,
                        iter_name,
                        sumo_env=self._env,
                    )
                    aggregator.run()
