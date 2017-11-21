# db_utils.py
"""

"""
import logging
from typing import Any, MutableMapping, Optional, Tuple

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s: %(name)s - [%(levelname)s] - %(message)s'
)


class DatabaseConnBase:
    def __init__(self, conn_func, conn_info: MutableMapping):
        self._conn = None
        self.conn_func = conn_func
        self.conn_info = conn_info

    def __del__(self):
        if self._conn:
            self._conn.close()

    def _initialize_connection(self):
        self._conn = self.conn_func(**self.conn_info)

    @property
    def conn(self):
        if not self._conn:
            self._initialize_connection()
        return self._conn

    @property
    def cursor(self):
        if not self._conn:
            self._initialize_connection()
        return self._conn.cursor()

    def close(self):
        if self._conn:
            self._conn.close()
        else:
            logger.warning('No activate connection to close.')

    def commit(self):
        if not self._conn:
            raise AttributeError(
                'The connection has not been initialized, unable to commit '
                'transaction.'
            )
        self._conn.commit()

    def execute(
            self,
            query_or_stmt: str,
            verbose: bool = True,
            has_res: bool = False,
            auto_commit: bool = False
    ) -> Optional[Tuple[Any]]:
        """
        Creates a new cursor object, and executes the query/statement.  If
        `has_res` is `True`, then it returns the list of tuple results.

        :param query_or_stmt: The query or statement to run.
        :param verbose: If `True`, prints out the statement or query to STDOUT.
        :param has_res: Whether or not results should be returned.
        :param auto_commit: Immediately commits the changes to the database
         after the execute is performed.

        :return: If `has_res` is `True`, then a list of tuples.
        """
        cur = self.cursor
        if verbose:
            logger.info(f'Using {cur}')
            logger.info(f'Executing:\n{query_or_stmt}')

        cur.execute(query_or_stmt)

        if auto_commit:
            logger.info('Committing transaction...')
            self.commit()

        if has_res:
            logger.info('Returning results...')
            return cur.fetchall()
