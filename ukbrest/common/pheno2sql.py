import csv
import os
import re
import sys
import tempfile
from subprocess import Popen, PIPE
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from sqlalchemy import create_engine
from sqlalchemy.types import TEXT, FLOAT, TIMESTAMP, INT

from ukbrest.common.utils.datagen import get_tmpdir
from ukbrest.config import logger, SQL_CHUNKSIZE_ENV


class Pheno2SQL:
    _RE_COLUMN_NAME_PATTERN = '(?i)c[0-9a-z_]+_[0-9]+_[0-9]+'
    RE_COLUMN_NAME = re.compile('({})'.format(_RE_COLUMN_NAME_PATTERN))

    _RE_FIELD_INFO_PATTERN = '(?i)c(?P<field_id>[0-9a-z_]+)_(?P<instance>[0-9]+)_(?P<array>[0-9]+)'
    RE_FIELD_INFO = re.compile(_RE_FIELD_INFO_PATTERN)

    _RE_FIELD_CODING_PATTERN = '(?i)Uses data-coding (?P<coding>[0-9]+) '
    RE_FIELD_CODING = re.compile(_RE_FIELD_CODING_PATTERN)

    _RE_FULL_COLUMN_NAME_RENAME_PATTERN = '^(?i)(?P<field>{})([ ]+([ ]*as[ ]+)?(?P<rename>[\w_]+))?$'.format(_RE_COLUMN_NAME_PATTERN)
    RE_FULL_COLUMN_NAME_RENAME = re.compile(_RE_FULL_COLUMN_NAME_RENAME_PATTERN)

    def __init__(self, ukb_csvs, db_uri, bgen_sample_file=None, table_prefix='ukb_pheno_',
                 n_columns_per_table=sys.maxsize, loading_n_jobs=-1, tmpdir=tempfile.mkdtemp(prefix='ukbrest'),
                 loading_chunksize=5000, sql_chunksize=None):
        """
        :param ukb_csvs:
        :param db_uri:
        :param table_prefix:
        :param n_columns_per_table:
        :param loading_n_jobs:
        :param tmpdir:
        :param loading_chunksize: number of lines to read when loading CSV files to the SQL database.
        :param sql_chunksize: when an SQL query is submited to get phenotypes, this parameteres indicates the
        chunksize (number of rows).
        """

        if isinstance(ukb_csvs, (tuple, list)):
            self.ukb_csvs = ukb_csvs
        else:
            self.ukb_csvs = (ukb_csvs,)

        self.bgen_sample_file = bgen_sample_file

        self.db_uri = db_uri
        self.db_engine = None

        parse_result = urlparse(self.db_uri)
        self.db_type = parse_result.scheme

        if self.db_type == 'sqlite':
            logger.warning('sqlite does not support parallel loading')
            self.db_file = self.db_uri.split(':///')[-1]
        elif self.db_type == 'postgresql':
            self.db_host = parse_result.hostname
            self.db_port = parse_result.port
            self.db_name = parse_result.path.split('/')[-1]
            self.db_user = parse_result.username
            self.db_pass = parse_result.password

        self.table_prefix = table_prefix
        self.n_columns_per_table = n_columns_per_table
        self.loading_n_jobs = loading_n_jobs
        self.tmpdir = tmpdir
        self.loading_chunksize = loading_chunksize

        self.sql_chunksize = sql_chunksize
        if self.sql_chunksize is None:
            logger.warning('{} was not set, no chunksize for SQL queries, what can lead to '
                           'memory problems.'.format(SQL_CHUNKSIZE_ENV))

        self._fields_dtypes = {}

        # this is a temporary variable that holds information about loading
        self._loading_tmp = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for f in os.listdir(self.tmpdir):
            os.remove(os.path.join(self.tmpdir, f))

    def _get_db_engine(self):
        if self.db_engine is None:
            if self.db_type != 'sqlite':
                kargs = {'pool_size': 10}
            else:
                kargs = {}

            self.db_engine = create_engine(self.db_uri, **kargs)

        return self.db_engine

    def _close_db_engine(self):
        if self.db_engine is not None:
            del(self.db_engine)
            self.db_engine = None

    def _get_table_name(self, column_range_index, csv_file_idx):
        return '{}{}_{:02d}'.format(self.table_prefix, csv_file_idx, column_range_index)

    def _chunker(self, seq, size):
        """
        Divides a sequence in chunks according to the given size.
        :param seq:
        :param size:
        :return:
        """
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def _get_db_columns_dtypes(self, ukbcsv_file):
        """
        Returns a Pandas-compatible type list with SQLAlchemy types for each column.

        :param ukbcsv_file:
        :return:
        """

        logger.info('Getting columns types')

        filename = os.path.splitext(ukbcsv_file)[0] + '.html'

        logger.info('Reading data types from {}'.format(filename))
        with open(filename, 'r', encoding='latin1') as f:
            tmp = pd.read_html(f, match='UDI', header=0, index_col=1, flavor='html5lib')

        logger.debug('Filling NaN values')
        df_types = tmp[0].loc[:, 'Type']
        df_types = df_types.fillna(method='ffill')

        df_descriptions = tmp[0].loc[:, 'Description']
        df_descriptions = df_descriptions.fillna(method='ffill')
        del tmp

        db_column_types = {}
        column_types = {}
        column_descriptions = {}
        column_codings = {}

        # open just to get columns
        csv_df = pd.read_csv(ukbcsv_file, index_col=0, header=0, nrows=1)
        columns = csv_df.columns.tolist()
        del csv_df

        logger.debug('Reading columns')
        for col in columns:
            col_type = df_types[col]
            final_db_col_type = TEXT

            if col_type == 'Continuous':
                final_db_col_type = FLOAT

            elif col_type == 'Integer':
                final_db_col_type = INT

            elif col_type in ('Date', 'Time'):
                final_db_col_type = TIMESTAMP

            db_column_types[col] = final_db_col_type
            column_types[self._rename_columns(col)] = col_type
            column_descriptions[self._rename_columns(col)] = df_descriptions[col].split('Uses data-coding ')[0]

            # search for column coding
            coding_matches = re.search(Pheno2SQL.RE_FIELD_CODING, df_descriptions[col])
            if coding_matches is not None:
                column_codings[self._rename_columns(col)] = int(coding_matches.group('coding'))

        return db_column_types, column_types, column_descriptions, column_codings

    def _rename_columns(self, column_name):
        if column_name == 'eid':
            return column_name

        return 'c{}'.format(column_name.replace('.', '_').replace('-', '_'))

    def _create_tables_schema(self, csv_file, csv_file_idx):
        """
        Reads the data types of each data field in csv_file and create the necessary database tables.
        :return:
        """
        logger.info('Creating database tables')

        tmp = pd.read_csv(csv_file, index_col=0, header=0, nrows=1, low_memory=False)
        old_columns = tmp.columns.tolist()
        del tmp
        new_columns = [self._rename_columns(x) for x in old_columns]

        all_columns = tuple(zip(old_columns, new_columns))
        # FIXME: check if self.n_columns_per_table is greater than the real number of columns
        self._loading_tmp['chunked_column_names'] = tuple(enumerate(self._chunker(all_columns, self.n_columns_per_table)))
        self._loading_tmp['chunked_table_column_names'] = \
            {self._get_table_name(col_idx, csv_file_idx): [col[1] for col in col_names]
             for col_idx, col_names in self._loading_tmp['chunked_column_names']}

        # get columns dtypes (for PostgreSQL and standard ones)
        db_types_old_column_names, all_fields_dtypes, all_fields_description, all_fields_coding = self._get_db_columns_dtypes(csv_file)
        db_dtypes = {self._rename_columns(k): v for k, v in db_types_old_column_names.items()}
        self._fields_dtypes.update(all_fields_dtypes)

        data_sample = pd.read_csv(csv_file, index_col=0, header=0, nrows=1, dtype=str)
        data_sample = data_sample.rename(columns=self._rename_columns)

        data_table_if_exist = 'replace'

        if csv_file_idx == 0:
            fields_table_if_exist = ['replace'] + ['append'] * (len(self._loading_tmp['chunked_column_names']) - 1)
        else:
            fields_table_if_exist = ['append'] * (len(self._loading_tmp['chunked_column_names']))

        current_stop = 0
        for column_names_idx, column_names in self._loading_tmp['chunked_column_names']:
            new_columns_names = [x[1] for x in column_names]

            fields_ids = []
            instances = []
            arrays = []
            fields_dtypes = []
            fields_descriptions = []
            fields_codings = []

            for col_name in new_columns_names:
                match = re.match(Pheno2SQL.RE_FIELD_INFO, col_name)

                fields_ids.append(match.group('field_id'))
                instances.append(int(match.group('instance')))
                arrays.append(int(match.group('array')))

                fields_dtypes.append(all_fields_dtypes[col_name])
                fields_descriptions.append(all_fields_description[col_name])

                if col_name in all_fields_coding:
                    fields_codings.append(all_fields_coding[col_name])
                else:
                    fields_codings.append(np.nan)


            # Create main table structure
            table_name = self._get_table_name(column_names_idx, csv_file_idx)
            logger.info('Table {} ({} columns)'.format(table_name, len(new_columns_names)))
            data_sample.loc[[], new_columns_names].to_sql(table_name, self._get_db_engine(), if_exists=data_table_if_exist, dtype=db_dtypes)

            # Create auxiliary table
            n_column_names = len(new_columns_names)
            current_start = current_stop
            current_stop = current_start + n_column_names

            aux_table = pd.DataFrame({
                'column_name': new_columns_names,
                'field_id': fields_ids,
                'inst': instances,
                'arr': arrays,
                'coding': fields_codings,
                'table_name': table_name,
                'type': fields_dtypes,
                'description': fields_descriptions
            })
            # aux_table = aux_table.set_index('column_name')
            aux_table.to_sql('fields', self._get_db_engine(), index=False, if_exists=fields_table_if_exist[column_names_idx])

    def _save_column_range(self, csv_file, csv_file_idx, column_names_idx, column_names):
        table_name = self._get_table_name(column_names_idx, csv_file_idx)
        output_csv_filename = os.path.join(get_tmpdir(self.tmpdir), table_name + '.csv')
        full_column_names = ['eid'] + [x[0] for x in column_names]

        data_reader = pd.read_csv(csv_file, index_col=0, header=0, usecols=full_column_names,
                                  chunksize=self.loading_chunksize, dtype=str)

        new_columns = [x[1] for x in column_names]

        logger.debug('{}'.format(output_csv_filename))

        write_headers = True
        if self.db_type == 'sqlite':
            write_headers = False

        for chunk_idx, chunk in enumerate(data_reader):
            chunk = chunk.rename(columns=self._rename_columns)
            # chunk = self._replace_null_str(chunk)

            if chunk_idx == 0:
                chunk.loc[:, new_columns].to_csv(output_csv_filename, quoting=csv.QUOTE_NONNUMERIC, na_rep=np.nan, header=write_headers, mode='w')
            else:
                chunk.loc[:, new_columns].to_csv(output_csv_filename, quoting=csv.QUOTE_NONNUMERIC, na_rep=np.nan, header=False, mode='a')

        return table_name, output_csv_filename

    def _create_temporary_csvs(self, csv_file, csv_file_idx):
        logger.info('Writing temporary CSV files')

        self._close_db_engine()
        self.table_csvs = Parallel(n_jobs=self.loading_n_jobs)(
            delayed(self._save_column_range)(csv_file, csv_file_idx, column_names_idx, column_names)
            for column_names_idx, column_names in self._loading_tmp['chunked_column_names']
        )

    def _load_single_csv(self, table_name, file_path):
        logger.info('{} -> {}'.format(file_path, table_name))

        if self.db_type == 'sqlite':
            statement = (
                '.mode csv\n' +
                '.separator ","\n' +
                '.headers on\n' +
                '.import {file_path} {table_name}\n'
            ).format(**locals())

            p = Popen(['sqlite3', self.db_file], stdout=PIPE, stdin=PIPE, stderr=PIPE)
            stdout_data, stderr_data = p.communicate(input=str.encode(statement))

            if p.returncode != 0:
                raise Exception(stdout_data + b'\n' + stderr_data)

            # For each column, set NULL rows with empty strings
            # FIXME: this codes needs refactoring
            for col_name in self._loading_tmp['chunked_table_column_names'][table_name]:
                statement = (
                    'update {table_name} set {col_name} = null where {col_name} == "nan";'
                ).format(**locals())

                p = Popen(['sqlite3', self.db_file], stdout=PIPE, stdin=PIPE, stderr=PIPE)
                stdout_data, stderr_data = p.communicate(input=str.encode(statement))

                if p.returncode != 0:
                    raise Exception(stdout_data + b'\n' + stderr_data)

        elif self.db_type == 'postgresql':
            statement = (
                "\copy {table_name} from '{file_path}' (format csv, header, null ('nan'))"
            ).format(**locals())

            current_env = os.environ.copy()
            current_env['PGPASSWORD'] = self.db_pass
            p = Popen(['psql', '-w', '-h', self.db_host, '-p', str(self.db_port),
                       '-U', self.db_user, '-d', self.db_name, '-c', statement],
                      stdout=PIPE, stderr=PIPE, env=current_env)
            stdout_data, stderr_data = p.communicate()

            if p.returncode != 0:
                raise Exception(stdout_data + b'\n' + stderr_data)

    def _load_csv(self):
        logger.info('Loading CSV files into database')

        if self.db_type != 'sqlite':
            self._close_db_engine()
            # parallel csv loading is only supported in databases different than sqlite
            Parallel(n_jobs=self.loading_n_jobs)(
                delayed(self._load_single_csv)(table_name, file_path)
                for table_name, file_path in self.table_csvs
            )
        else:
            for table_name, file_path in self.table_csvs:
                self._load_single_csv(table_name, file_path)

    def _load_bgen_samples(self):
        if self.bgen_sample_file is None or not os.path.isfile(self.bgen_sample_file):
            logger.warning('BGEN sample file not set or does not exist: {}'.format(self.bgen_sample_file))
            return

        logger.info('Loading BGEN sample file: {}'.format(self.bgen_sample_file))

        samples_data = pd.read_table(self.bgen_sample_file, sep=' ', header=0, usecols=['ID_1', 'ID_2'], skiprows=[1])
        samples_data.set_index(np.arange(1, samples_data.shape[0] + 1), inplace=True)
        samples_data.drop('ID_2', axis=1, inplace=True)
        samples_data.rename(columns={'ID_1': 'eid'}, inplace=True)

        samples_data.to_sql('samples', self._get_db_engine(), if_exists='replace')

    def _run_psql(self, sql_statement):
        current_env = os.environ.copy()
        current_env['PGPASSWORD'] = self.db_pass
        p = Popen(['psql', '-w', '-h', self.db_host, '-p', str(self.db_port),
                   '-U', self.db_user, '-d', self.db_name, '-c', sql_statement],
                  stdout=PIPE, stderr=PIPE, env=current_env)
        stdout_data, stderr_data = p.communicate()

        if p.returncode != 0:
            raise Exception(stdout_data + b'\n' + stderr_data)

    def _load_events(self):
        if self.db_type == 'sqlite':
            logger.warning('Events loading is not supported in SQLite')
            return

        logger.info('Loading events table')

        # create table
        db_engine = self._get_db_engine()

        create_events_table_sql = """
            DROP TABLE IF EXISTS events;
            CREATE TABLE events
            (
                eid bigint NOT NULL,
                field_id integer NOT NULL,
                instance integer NOT NULL,
                event text COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT pk_events PRIMARY KEY (eid, field_id, instance, event)
            )
            WITH (
                OIDS = FALSE
            );
        """

        with db_engine.connect() as con:
            con.execute(create_events_table_sql)

        # insert data of categorical multiple fields
        categorical_variables = pd.read_sql("""
            select column_name, field_id, inst, table_name
            from fields
            where type = 'Categorical (multiple)'
        """, self._get_db_engine())

        for (field_id, field_instance), field_data in categorical_variables.groupby(by=['field_id', 'inst']):
            sql_st = """
                insert into events (eid, field_id, instance, event)
                (
                    select distinct *
                    from (
                        select eid, {field_id}, {field_instance}, unnest(array[{field_columns}]) as event
                        from {tables}
                    ) t
                    where t.event is not null
                )
            """.format(
                field_id=field_id,
                field_instance=field_instance,
                field_columns=', '.join([cn for cn in set(field_data['column_name'])]),
                tables=self._create_joins(list(set(field_data['table_name'])), join_type='inner join'),
            )

            with db_engine.connect() as con:
                con.execute(sql_st)

    def _create_constraints(self):
        if self.db_type == 'sqlite':
            logger.warning('Indexes are not supported for SQLite')
            return

        logger.info('Creating table constraints (indexes, primary keys, etc)')

        # fields table
        with self._get_db_engine().connect() as conn:
            pk_sql = """
                ALTER TABLE fields ADD CONSTRAINT pk_fields PRIMARY KEY (column_name);
            """
            conn.execute(pk_sql)

            for column in ('field_id', 'inst', 'arr', 'table_name', 'type', 'coding'):
                index_sql = """
                    CREATE INDEX ix_fields_{column_name}
                    ON fields USING btree
                    ({column_name})
                """.format(column_name=column)

                conn.execute(index_sql)

        # events table
        with self._get_db_engine().connect() as conn:
            index_sql = """
                CREATE INDEX ix_events_event
                ON events USING btree
                (event)
            """

            conn.execute(index_sql)

    def load_data(self):
        """
        Load self.ukb_csv into the database configured.
        :return:
        """
        logger.info('Loading phenotype data into database')

        for csv_file_idx, csv_file in enumerate(self.ukb_csvs):
            logger.info('Working on {}'.format(csv_file))

            self._create_tables_schema(csv_file, csv_file_idx)
            self._create_temporary_csvs(csv_file, csv_file_idx)
            self._load_csv()

        self._load_bgen_samples()
        self._load_events()
        self._create_constraints()

        # delete temporary variable
        del(self._loading_tmp)

        logger.info('Loading finished!')

    def initialize(self):
        logger.info('Initializing')

        logger.info('Loading fields dtypes')
        self.init_field_dtypes()

        logger.info('Initialization finished!')

    def _create_joins(self, tables, join_type='inner join'):
        if len(tables) == 1:
            return tables[0]

        return tables[0] + ' ' + ' '.join(['{join_type} {table} using (eid) '.format(join_type=join_type, table=t) for t in tables[1:]])

    def _get_needed_tables(self, all_columns):
        all_columns_quoted = ["'{}'".format(x.replace("'", "''")) for x in all_columns]

        # FIXME: are parameters correctly escaped by the arg parser?
        tables_needed_df = pd.read_sql(
            'select distinct table_name '
            'from fields '
            'where column_name in (' + ','.join(all_columns_quoted) + ')',
        self._get_db_engine()).loc[:, 'table_name'].tolist()

        if len(tables_needed_df) == 0:
            raise Exception('Tables not found.')

        return tables_needed_df

    def get_field_dtype(self, field=None):
        """Returns the type of the field. If field is None, then it just loads all fields types"""

        if field in self._fields_dtypes:
            return self._fields_dtypes[field]

        # initialize dbtypes for all fields
        field_type = pd.read_sql(
            'select distinct column_name, type '
            'from fields',
        self._get_db_engine())

        for row in field_type.itertuples():
            self._fields_dtypes[row.column_name] = row.type

        return self._fields_dtypes[field] if field in self._fields_dtypes else None


    def _get_fields_from_reg_exp(self, ecolumns):
        if ecolumns is None:
            return []

        where_st = ["column_name ~ '{}'".format(ecol) for ecol in ecolumns]
        select_st = """
            select distinct column_name
            from fields
            where {}
            order by column_name
        """.format(' or '.join(where_st))

        return pd.read_sql(select_st, self._get_db_engine()).loc[:, 'column_name'].tolist()

    def _get_fields_from_statements(self, statement):
        """This method gets all fields mentioned in the statements."""
        columns_fields = []
        if statement is not None:
            columns_fields = list(set([x for col in statement for x in re.findall(Pheno2SQL.RE_COLUMN_NAME, col)]))

        return columns_fields

    def _get_integer_fields(self, columns):
        """This method returns a list of fields (either its column specification, like c64_0_0 or its rename like
        myfield) that are of type integer."""
        int_columns = []

        for col in columns:
            if col == 'eid':
                continue

            match = re.search(Pheno2SQL.RE_FULL_COLUMN_NAME_RENAME, col)

            if match is None:
                continue

            col_field = match.group('field')

            if self.get_field_dtype(col_field) != 'Integer':
                continue

            # select rename first, if not specified select field column
            col_rename = next((grp_val for grp_val in (match.group('rename'), match.group('field')) if grp_val is not None))
            int_columns.append(col_rename)

        return int_columns

    def query(self, columns=None, ecolumns=None, filterings=None, order_by=None):
        # select needed tables to join
        columns_fields = self._get_fields_from_statements(columns)
        reg_exp_columns_fields = self._get_fields_from_reg_exp(ecolumns)
        filterings_columns_fields = self._get_fields_from_statements(filterings)

        tables_needed_df = self._get_needed_tables(columns_fields + reg_exp_columns_fields + filterings_columns_fields)

        all_columns = ['eid'] + (columns if columns is not None else []) + reg_exp_columns_fields
        int_columns = self._get_integer_fields(all_columns)

        base_sql = """
            select {data_fields}
            from {tables_join}
            {where_statements}
        """

        if order_by is not None:
            outer_sql = """
                select {data_fields}
                from {order_by} s left outer join (
                    {base_sql}
                ) u
                using (eid)
                order by s.index asc
            """.format(order_by=order_by, base_sql=base_sql, data_fields='{data_fields}')

            base_sql = outer_sql

        # FIXME: are parameters correctly escaped by the arg parser?
        results_iterator = pd.read_sql(
            base_sql.format(
                data_fields=','.join(all_columns),
                tables_join=self._create_joins(tables_needed_df, join_type='full outer join'),
                where_statements=((' where ' + ' and '.join(filterings)) if filterings is not None else ''),
            ),
            self._get_db_engine(), index_col='eid', chunksize=self.sql_chunksize
        )

        if self.sql_chunksize is None:
            results_iterator = iter([results_iterator])

        for chunk in results_iterator:
            for col in int_columns:
                chunk[col] = chunk[col].map(lambda x: np.nan if pd.isnull(x) else '{:1.0f}'.format(x))

            yield chunk

    def query_yaml_fields(self, yaml_file, section, order_by=None):
        section_data = yaml_file[section]

        include_only_stmts = None
        if 'samples_include_only' in yaml_file:
            include_only_stmts = yaml_file['samples_include_only']

        section_field_statements = [v for x in section_data for k, v in x.items()]

        for chunk in self.query(section_field_statements, filterings=include_only_stmts, order_by=order_by):
            chunk = chunk.rename(columns={v:k for x in section_data for k, v in x.items()})
            yield chunk

    def query_yaml_case_control(self, yaml_file, section, order_by=None):
        section_data = yaml_file[section]

        for column_data in section_data:
            for column_name, column_fields_data in column_data.items():
                for field_data in column_fields_data:
                    for field_name, field_conditions in field_data.items():
                        base_query = "select eid from {0} where event in ({1}) group by eid"

                        sql_cases_st = """
                            select s.index, et.eid, 1 as iscase
                            from events_84 inner join samples s on (s.id = eid)
                            group by s.index, eid
                        """

                        yield pd.DataFrame({'hello': [1,2,3], 'now': ['one', 'two', 'three']})

    def query_yaml(self, yaml_file, section, order_by=None):
        if section in ('fields', 'covariates'):
            return self.query_yaml_fields(yaml_file, section, order_by)
        elif section == 'case_control':
            return self.query_yaml_case_control(yaml_file, section, order_by)
        else:
            raise ValueError('Invalid section value')
