import json
import os
from collections import defaultdict

import psycopg2
import psycopg2.extras
import six
from six.moves.urllib import parse as urlparse

from readinglist import logger
from readinglist.storage import StorageBase, exceptions
from readinglist.utils import classname, COMPARISON


psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


class DBResults(object):
    """Wrapper around psycopg cursor objects.

    :note:
        Main idea comes from the queries library, by Gavin M. Roy.
        https://pypi.python.org/pypi/queries, released under BSD License.
    """
    def __init__(self, cursor):
        self.cursor = cursor

    def __iter__(self):
        """Iterate through the result set

        :rtype: mixed
        """
        if self.cursor.rowcount > 0:
            self.cursor.scroll(0, 'absolute')  # rewind
            for row in self.cursor:
                yield row

        self.cursor.close()
        raise StopIteration

    def __getitem__(self, item):
        """Fetch an individual row from the result set

        :rtype: mixed
        :raises: IndexError
        """
        try:
            self.cursor.scroll(item, 'absolute')
        except psycopg2.ProgrammingError:
            raise IndexError('No such row')
        else:
            return self.cursor.fetchone()

    def __len__(self):
        """Return the number of rows that were returned from the query

        :rtype: int
        """
        return self.cursor.rowcount if self.cursor.rowcount >= 0 else 0


class PostgreSQL(StorageBase):

    def __init__(self, *args, **kwargs):
        super(PostgreSQL, self).__init__(*args, **kwargs)
        self._conn = psycopg2.connect(**kwargs)
        self._cursor = None
        self.init_schema()

    @property
    def cursor(self):
        if self._cursor is None or self._cursor.closed:
            self._cursor = self._get_cursor()
        return self._cursor

    def _get_cursor(self):
        options = dict(cursor_factory=psycopg2.extras.DictCursor)
        return self._conn.cursor(**options)

    def _escape(self, query, placeholders):
        return self.cursor.mogrify(query, placeholders)

    def _execute(self, query, commit=True, silent=False, **kwargs):
        """Run the specified query, commit by default and return the
        result set.
        """
        try:
            self.cursor.execute(query, kwargs)
        except psycopg2.Error as e:
            self._conn.rollback()
            self.cursor.close()
            if not silent:
                logger.debug(self._escape(query, kwargs))
                logger.exception(e)
            raise

        if commit:
            self._conn.commit()

        return DBResults(self.cursor)

    def init_schema(self):
        """Create PostgreSQL tables, only if not exists.

        :note:
            Relies on JSON fields, available in recent versions of PostgreSQL.
        """
        # Since indices cannot be created with IF NOT EXISTS, inspect:
        try:
            inspect_tables = "SELECT * FROM records LIMIT 0;"
            self._execute(inspect_tables, silent=True)
            exists = True
        except psycopg2.Error:
            exists = False

        if exists:
            logger.debug('Detected PostgreSQL storage tables')
            return

        # Make sure database is UTF-8
        query = """
        SELECT pg_encoding_to_char(encoding) AS encoding
          FROM pg_database
         WHERE datname =  current_database();
        """
        result = self._execute(query)
        encoding = result[0]['encoding'].lower()
        assert encoding == 'utf8', 'Unexpected database encoding %s' % encoding

        # Create schema
        here = os.path.abspath(os.path.dirname(__file__))
        schema = open(os.path.join(here, 'schema.sql')).read()
        self._execute(schema)
        logger.info('Created PostgreSQL storage tables')

    def flush(self):
        """Delete records from tables without destroying schema. Mainly used
        in tests suites.
        """
        query = """
        DELETE FROM deleted;
        DELETE FROM records;
        """
        self._execute(query)
        logger.debug('Flushed PostgreSQL storage tables')

    def ping(self):
        try:
            self._execute("SELECT now();")
            return True
        except psycopg2.Error:
            return False

    def collection_timestamp(self, resource, user_id):
        query = """
        SELECT resource_timestamp(%(user_id)s, %(resource_name)s)::BIGINT
            AS timestamp;
        """
        resource_name = classname(resource)
        placeholders = dict(user_id=user_id, resource_name=resource_name)
        result = self._execute(query, **placeholders)
        return result[0]['timestamp']

    def create(self, resource, user_id, record):
        # This will start transaction
        self.check_unicity(resource, user_id, record)
        # XXX - can raise unicityerror: ok ?

        query = """
        INSERT INTO records (user_id, resource_name, data)
        VALUES (%(user_id)s, %(resource_name)s, %(data)s::json)
        RETURNING id, last_modified::BIGINT
        """
        resource_name = classname(resource)
        placeholders = dict(user_id=user_id,
                            resource_name=resource_name,
                            data=json.dumps(record))
        inserted = self._execute(query, **placeholders)
        inserted = inserted[0]

        record = record.copy()
        record[resource.id_field] = inserted['id']
        record[resource.modified_field] = inserted['last_modified']
        return record

    def get(self, resource, user_id, record_id):
        query = """
        SELECT last_modified::BIGINT, data
          FROM records
         WHERE id = %(record_id)s
        """
        placeholders = dict(record_id=record_id)
        results = self._execute(query, **placeholders)
        try:
            result = results[0]
        except IndexError:
            raise exceptions.RecordNotFoundError(record_id)

        record = result['data']
        record[resource.id_field] = record_id
        record[resource.modified_field] = result['last_modified']
        return record

    def update(self, resource, user_id, record_id, record):
        record = record.copy()
        record[resource.id_field] = record_id

        # Create or update ?
        try:
            # This will start a transaction
            self.get(resource, user_id, record_id)
            create = False
        except exceptions.RecordNotFoundError:
            create = True

        self.check_unicity(resource, user_id, record)
        # XXX - can raise: abort!

        if create:
            query = """
            INSERT INTO records (id, user_id, resource_name, data)
            VALUES (%(record_id)s, %(user_id)s,
                    %(resource_name)s, %(data)s::json)
            RETURNING last_modified::BIGINT
            """
        else:
            query = """
            UPDATE records SET data=%(data)s::json
            WHERE id = %(record_id)s
            RETURNING last_modified::BIGINT
            """
        resource_name = classname(resource)
        placeholders = dict(record_id=record_id,
                            user_id=user_id,
                            resource_name=resource_name,
                            data=json.dumps(record))
        results = self._execute(query, **placeholders)

        result = results[0]
        record[resource.modified_field] = result['last_modified']
        return record

    def delete(self, resource, user_id, record_id):
        query = """
        DELETE
        FROM records
        WHERE id = %(record_id)s
        RETURNING id
        """
        placeholders = dict(record_id=record_id)
        deleted = self._execute(query, commit=False, **placeholders)
        if len(deleted) == 0:
            raise exceptions.RecordNotFoundError(record_id)

        query = """
        INSERT INTO deleted (id, user_id, resource_name)
        VALUES (%(record_id)s, %(user_id)s, %(resource_name)s)
        RETURNING last_modified::BIGINT
        """
        resource_name = classname(resource)
        placeholders = dict(record_id=record_id,
                            user_id=user_id,
                            resource_name=resource_name)
        inserted = self._execute(query, **placeholders)

        inserted = inserted[0]
        record = {}
        record[resource.modified_field] = inserted['last_modified']
        record[resource.id_field] = record_id

        field, value = resource.deleted_mark
        record[field] = value
        return record

    def get_all(self, resource, user_id, filters=None, sorting=None,
                pagination_rules=None, limit=None, include_deleted=False):
        query = """
        WITH collection_filtered AS (
            SELECT id, last_modified::BIGINT, data
              FROM records
             WHERE user_id = %%(user_id)s
               AND resource_name = %%(resource_name)s
               %(conditions_filter)s
        ),
        total_filtered AS (
            SELECT COUNT(*) AS count
              FROM collection_filtered
        ),
        fake_deleted AS (
            SELECT %%(deleted_mark)s::json AS data
        ),
        filtered_deleted AS (
            SELECT id, last_modified::BIGINT, fake_deleted.data AS data
              FROM deleted, fake_deleted
             WHERE user_id = %%(user_id)s
               AND resource_name = %%(resource_name)s
               %(conditions_filter)s
               %(deleted_limit)s
        ),
        all_records AS (
            SELECT * FROM filtered_deleted
             UNION ALL
            SELECT * FROM collection_filtered
        ),
        paginated_records AS (
            SELECT DISTINCT id
              FROM all_records
              %(pagination_rules)s
        )
        SELECT total_filtered.count AS count_total, a.*
          FROM paginated_records AS p JOIN all_records AS a ON (a.id = p.id),
               total_filtered
          %(sorting)s
          %(pagination_limit)s;
        """
        resource_name = classname(resource)
        deleted_mark = json.dumps(dict([resource.deleted_mark]))
        placeholders = dict(user_id=user_id,
                            resource_name=resource_name,
                            deleted_mark=deleted_mark)

        safeholders = defaultdict(six.text_type)

        if filters:
            conditions = [self._format_condition(resource, f) for f in filters]
            and_conditions = ' AND '.join(conditions)
            safeholders['conditions_filter'] = 'AND %s' % and_conditions

        if not include_deleted:
            safeholders['deleted_limit'] = 'LIMIT 0'

        if sorting:
            sorts = [self._format_sort(resource, s) for s in sorting]
            sorts = ', '.join(sorts)
            safeholders['sorting'] = 'ORDER BY %s' % sorts

        if pagination_rules:
            rules = []
            for rule in pagination_rules:
                conditions = [self._format_condition(resource, r)
                              for r in rule]
                rules.append(' AND '.join(conditions))

            or_rules = ' OR '.join(['(%s)' % r for r in rules])
            safeholders['pagination_rules'] = 'WHERE %s' % or_rules

        if limit:
            safe = self._escape('LIMIT %s', (limit,))
            safeholders['pagination_limit'] = safe

        results = self._execute(query % safeholders, **placeholders)

        try:
            count_total = results[0]['count_total']
        except IndexError:
            return [], 0

        records = []
        for result in results:
            record = result['data']
            record[resource.id_field] = result['id']
            record[resource.modified_field] = result['last_modified']
            records.append(record)

        return records, count_total

    def _format_condition(self, resource, filter_):
        """Format the filter in SQL.

        :note:

            Field name and value are escaped as they come from HTTP API.
        """
        field, value, operator = filter_

        operators = {
            COMPARISON.EQ: '=',
            COMPARISON.NOT: '<>',
        }
        operator = operators.setdefault(operator, operator)

        if field == resource.id_field:
            field = 'id'
        elif field == resource.modified_field:
            field = 'last_modified::BIGINT'
        else:
            # JSON operator ->> retrieves values as text.
            # If field is missing, we default to 'null'.
            field = self._escape("coalesce(data->>%s, '')", (field,))
            # JSON-ify the native value (e.g. True -> 'true')
            value = json.dumps(value).strip('"')

        safe = "%s %s %%s" % (field, operator)
        return self._escape(safe, (value,))

    def _format_sort(self, resource, sort):
        """Format the sort in SQL instruction.

        :note:

            Field name is escaped as it comes from HTTP API.
        """
        field, direction = sort
        direction = 'ASC' if direction > 0 else 'DESC'

        if field == resource.id_field:
            field = 'id'
        elif field == resource.modified_field:
            field = 'last_modified'
        else:
            field = self._escape("data->>%s", (field,))

        safe = "%s %s" % (field, direction)
        return safe


def load_from_config(config):
    settings = config.registry.settings
    uri = settings.get('storage.url', '')
    uri = urlparse.urlparse(uri)
    return PostgreSQL(host=uri.hostname or 'localhost',
                      port=uri.port or 5432,
                      user=uri.username or 'postgres',
                      password=uri.password or 'postgres',
                      database=uri.path[1:] if uri.path else 'postgres')
