"""Contains classes for aggregation of tables"""
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
        content: str,
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
        sumo_env = kwargs.get("sumo_env", "prod")
        self._sumo = SumoClient(sumo_env, token)
        self._content = content
        self._case_identifier = ut.return_uuid(self._sumo, case_identifier)
        self._name = name
        self.loop = asyncio.get_event_loop()
        self._iteration = iteration
        # try:
        (
            self._parent_id,
            self._object_ids,
            self._meta,
            self._real_ids,
            self._p_meta,
            self._table_index,
        ) = ut.query_for_table(
            self.sumo,
            self._case_identifier,
            self._name,
            tag,
            self._iteration,
            content,
            **kwargs
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
    def real_ids(self) -> list:
        """Return _real_ids attribute"""
        return self._real_ids

    @property
    def parameters(self) -> dict:
        """Return the _p_meta attribute"""
        return self._p_meta

    @property
    def base_meta(self) -> dict:
        """Return _meta attribute"""
        return self._meta

    @property
    def aggregated(self) -> pd.DataFrame:
        """Return the _aggregated attribute"""
        if self._aggregated is None:
            self.aggregate()

        return self._aggregated

    @aggregated.setter
    def aggregated(self, aggregated):
        """Set the _aggregated attribute

        Args:
            aggregated (pa.Table): aggregated results
        """
        self._aggregated = aggregated

    @ut.timethis("aggregation")
    def aggregate(self):
        """Aggregate objects over tables per real stored in sumo"""
        if self.table_index is not None:
            self.aggregated = self.loop.run_until_complete(
                ut.aggregate_arrow(self.object_ids, self.sumo, self.loop)
            )
        else:
            self.aggregated = None
            print("No aggregation will be done, no table index!!")

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
            print("No aggregation in place, so no upload will be done!!")
