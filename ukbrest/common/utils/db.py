from sqlalchemy import create_engine
import sqlite3

def create_table(table_name, columns, db_engine, constraints=None, drop_if_exists=True):
    with db_engine.connect() as conn:
        drop_st="DROP TABLE IF EXISTS {0};".format(table_name) if drop_if_exists else ''
        #with_st = """WITH (
        #        OIDS = FALSE
        #   );"""
        sql_st = """
            CREATE TABLE {create_if_not_exists} {table_name}
            (
                {columns}
                {constraints}
            );
        """.format(
            create_if_not_exists='if not exists' if not drop_if_exists else '',
            table_name=table_name,
            columns=',\n'.join(columns),
            # FIXME support for more than one constraint
            constraints=',CONSTRAINT {}'.format(constraints[0]) if constraints is not None else ''
        )
        conn.execute(drop_st)
        conn.execute(sql_st)
        # conn.execute(with_st)
        # cursor = conn.cursor()
	
        # cursor.executescript(sql_st)


def create_indexes(table_name, columns, db_engine):
    with db_engine.connect() as conn:
        for column_spec in columns:

            if not isinstance(column_spec, (tuple, list)):
                column_spec = (column_spec,)

            index_name_suffix = '_'.join(column_spec)
            columns_name = ', '.join(column_spec)

            index_sql = """
                CREATE INDEX ix_{table_name}_{index_name_suffix}
                ON {table_name} USING btree
                ({columns_name})
            """.format(table_name=table_name, index_name_suffix=index_name_suffix, columns_name=columns_name)

            conn.execute(index_sql)


class DBAccess():
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.db_engine = None

    def _close_db_engine(self):
        if self.db_engine is not None:
            self.db_engine.dispose()
            del(self.db_engine)
            self.db_engine = None

    def _get_db_engine(self):
        if self.db_engine is None:
            if self.db_uri is None or self.db_uri == "":
                raise ValueError('DB URI was not set')
            try:
                self.db_engine = create_engine(self.db_uri, pool_size=10)
            except TypeError:
                self.db_engine = create_engine(self.db_uri, echo=True)
        return self.db_engine

    def _vacuum(self, table_name):
        with self._get_db_engine().connect().execution_options(isolation_level="SERIALIZABLE") as conn:
            conn.execute("vacuum;")
            conn.execute("analyze;")
