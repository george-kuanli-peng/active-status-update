"""Database management"""
import logging
import sqlite3
from typing import Optional


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DBManager:
    """Lazy db connection initialization"""

    def __init__(self,
                 stats_db: Optional[str] = None,
                 member_db: Optional[str] = None) -> None:
        self.stats_db_path = stats_db
        self.stats_db_conn = None
        self.member_db_path = member_db
        self.member_db_conn = None

    @property
    def stats_db(self) -> sqlite3.Connection:
        if self.stats_db_conn is None:
            self.stats_db_conn = sqlite3.connect(self.stats_db_path)
        return self.stats_db_conn

    @property
    def mem_db(self) -> sqlite3.Connection:
        if self.member_db_conn is None:
            self.member_db_conn = sqlite3.connect(self.member_db_path)
        return self.member_db_conn
