from os.path import join, basename, splitext
from glob import glob
import re

import pandas as pd

from ukbrest.common.utils.db import create_table, create_indexes, DBAccess
from ukbrest.config import logger


class Postloader(DBAccess):
    def __init__(self, db_uri):
        super(Postloader, self).__init__(db_uri)

        self.patterns = {
            'points': re.compile('[\.]{1,}')
        }

    def load_codings(self, codings_dir):
        db_engine = self._get_db_engine()

        create_table('codings',
            columns=[
                'data_coding bigint NOT NULL',
                'coding text NOT NULL',
                'meaning text NOT NULL',
                'node_id bigint NULL',
                'parent_id bigint NULL',
                'selectable boolean NULL',
            ],
            constraints=[
                'pk_codings PRIMARY KEY (data_coding, coding, meaning)'
            ],
            db_engine=self._get_db_engine()
         )

        for afile in glob(join(codings_dir, '*.tsv')):
            data = pd.read_table(afile)

            data_coding = int(splitext(basename(afile))[0].split('_')[1])
            data['data_coding'] = data_coding

            data.to_sql('codings', db_engine, if_exists='append', index=False)

        create_indexes('codings', ['data_coding', 'coding', 'node_id', 'parent_id', 'selectable'], db_engine=db_engine)

        self._vacuum('codings')

    def _rename_column(self, column_name):
        return re.sub(self.patterns['points'], '_', column_name.lower()).strip('_')

    def _get_column_type(self, pandas_type):
        if pandas_type == str:
            return 'Text'
        elif pandas_type == int:
            return 'Integer'
        elif pandas_type == float:
            return 'Continuous'
        else:
            return 'Text'

    def load_samples_data(self, data_dir, identifier_columns={}, skip_columns={}, separators={}):
        db_engine = self._get_db_engine()

        for afile in glob(join(data_dir, '*.txt')):
            filename = basename(afile)
            logger.info('Loading samples data from file: {}'.format(filename))

            sep = separators[filename] if filename in separators else ' '

            data = pd.read_table(afile, sep=sep)

            if filename in skip_columns:
                logger.info('Dropping columns: {}'.format(','.join(skip_columns[filename])))
                data = data.drop(skip_columns[filename], axis=1)

            eid_column = identifier_columns[filename] if filename in identifier_columns else 'eid'

            if eid_column not in data.columns:
                logger.error("File '{0}' has no identifier column ({1})".format(filename, eid_column))
                continue

            table_name = splitext(filename)[0]

            columns_rename = {old_col: self._rename_column(old_col) for old_col in data.columns}
            columns_rename[eid_column] = 'eid'
            data = data.rename(columns=columns_rename)

            data.to_sql(table_name, db_engine, if_exists='replace', index=False)

            # add primary key
            logger.info('Adding primary key')
            with db_engine.connect() as conn:
                conn.execute("""
                    ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY (eid);
                """.format(table_name=table_name))

            # insert new data columns into fields table
            logger.info("Adding columns to 'fields' table")
            columns_to_fields = [x for x in data.columns if x != 'eid']
            columns_dtypes_to_fields = [self._get_column_type(x) for ix, x in enumerate(data.dtypes) if data.columns[ix] != 'eid']

            fields_table_data = pd.DataFrame({
                'column_name': columns_to_fields,
                'field_id': columns_to_fields,
                'table_name': table_name,
                'type': columns_dtypes_to_fields,
            })

            fields_table_data.to_sql('fields', db_engine, index=False, if_exists='append')
