import json
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


class PostgreSQL(StorageBase):

    def __init__(self, *args, **kwargs):
        super(PostgreSQL, self).__init__(*args, **kwargs)
        self._conn = psycopg2.connect(**kwargs)
        self.init_schema()

    def _execute(self, query, placeholders=tuple(), fetch=True, commit=False, silent=False):

        options = dict(cursor_factory=psycopg2.extras.DictCursor)
        cursor = self._conn.cursor(**options)

        try:
            cursor.execute(query, placeholders)
        except psycopg2.Error as e:
            self._conn.rollback()
            if not silent:
                logger.exception(e)
                logger.debug(cursor.mogrify(query, placeholders))
            raise

        resultset = None
        if fetch:
            # XXX : use server side cursers instead
            resultset = cursor.fetchall()
        if commit:
            self._conn.commit()
        cursor.close()
        return resultset

    def init_schema(self):
        """Create PostgreSQL tables.

        :note:
            Relies on JSON fields, available in recent versions of PostgreSQL.
        """
        # Since indices cannot be created with IF NOT EXISTS, inspect:
        try:
            inspect_tables = "SELECT * FROM records LIMIT 0;"
            self._execute(inspect_tables, fetch=False, silent=True)
            exists = True
        except psycopg2.Error:
            exists = False

        if exists:
            logger.debug('Detected PostgreSQL storage tables')
            return

        query = """
        --
        -- Actual records
        --
        CREATE TABLE IF NOT EXISTS records (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(256) NOT NULL,
            resource_name  VARCHAR(256) NOT NULL,
            last_modified TIMESTAMP NOT NULL DEFAULT localtimestamp,
            data JSON NOT NULL DEFAULT '{}',
            UNIQUE (user_id, resource_name, last_modified)
        );
        CREATE INDEX idx_records_user_id ON records(user_id);
        CREATE INDEX idx_records_resource_name ON records(resource_name);
        CREATE INDEX idx_records_last_modified ON records(last_modified);

        --
        -- Deleted records
        --
        CREATE TABLE IF NOT EXISTS deleted (
            id INT4,
            user_id VARCHAR(256) NOT NULL,
            resource_name  VARCHAR(256) NOT NULL,
            last_modified TIMESTAMP NOT NULL,
            UNIQUE (user_id, resource_name, last_modified)
        );
        CREATE UNIQUE INDEX idx_deleted_id ON deleted(id);
        CREATE INDEX idx_deleted_user_id ON deleted(user_id);
        CREATE INDEX idx_deleted_resource_name ON deleted(resource_name);
        CREATE INDEX idx_deleted_last_modified ON deleted(last_modified);

        --
        -- Metadata table
        --
        CREATE TABLE IF NOT EXISTS metadata (
            name VARCHAR(128) NOT NULL,
            value VARCHAR(512) NOT NULL
        );
        INSERT INTO metadata (name, value) VALUES ('created_at', NOW()::TEXT);

        --
        -- Helpers
        --
        CREATE OR REPLACE FUNCTION as_epoch(ts TIMESTAMP) RETURNS BIGINT AS $$
        BEGIN
            RETURN (EXTRACT(EPOCH FROM ts) * 1000)::BIGINT;
        END;
        $$ LANGUAGE plpgsql;

        CREATE CAST (TIMESTAMP AS BIGINT)
           WITH FUNCTION as_epoch(TIMESTAMP) AS ASSIGNMENT;
        """
        self._execute(query, fetch=False)
        logger.info('Created PostgreSQL storage tables')

    def flush(self):
        query = """
        DELETE FROM deleted;
        DELETE FROM records;
        """
        self._execute(query, fetch=False, commit=True)
        logger.debug('Flushed PostgreSQL storage tables')

    def ping(self):
        try:
            self._execute("SELECT now();")
            return True
        except psycopg2.Error:
            return False

    def collection_timestamp(self, resource, user_id):
        query = """
        WITH max_records AS (
            SELECT MAX(last_modified) AS max
              FROM records
             WHERE user_id = %(user_id)s
               AND resource_name = %(resource_name)s
        ),
        max_deleted AS (
            SELECT MAX(last_modified) AS max
              FROM records
             WHERE user_id = %(user_id)s
               AND resource_name = %(resource_name)s
        )
        SELECT GREATEST(max_records.max, max_deleted.max)::BIGINT AS max
        FROM max_records, max_deleted
        UNION ALL
        SELECT localtimestamp::BIGINT AS max;
        """
        resource_name = classname(resource)
        placeholders = dict(user_id=user_id, resource_name=resource_name)
        latests = self._execute(query, placeholders=placeholders)
        return latests[0]['max'] or latests[1]['max']

    def create(self, resource, user_id, record):
        # This will start transaction
        self.check_unicity(resource, user_id, record)
        # XXX - can raise unicityerror: ok ?

        query = """
        INSERT INTO records (user_id, resource_name, data)
        VALUES (%(user_id)s, %(resource_name)s, %(data)s)
        RETURNING id, last_modified::BIGINT
        """
        resource_name = classname(resource)
        placeholders = dict(user_id=user_id,
                            resource_name=resource_name,
                            data=json.dumps(record))
        inserted = self._execute(query, placeholders=placeholders, commit=True)
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
        results = self._execute(query, placeholders=placeholders)
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
            VALUES (%(record_id)s, %(user_id)s, %(resource_name)s, %(data)s)
            RETURNING last_modified::BIGINT
            """
        else:
            query = """
            UPDATE records SET data=%(data)s, last_modified=localtimestamp
            WHERE id = %(record_id)s
            RETURNING last_modified::BIGINT
            """
        resource_name = classname(resource)
        placeholders = dict(record_id=record_id,
                            user_id=user_id,
                            resource_name=resource_name,
                            data=json.dumps(record))
        results = self._execute(query, placeholders=placeholders, commit=True)

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
        deleted = self._execute(query, placeholders=placeholders, commit=False)
        if not deleted:
            raise exceptions.RecordNotFoundError(record_id)

        query = """
        INSERT INTO deleted (id, user_id, resource_name, last_modified)
        VALUES (%(record_id)s, %(user_id)s, %(resource_name)s, localtimestamp)
        RETURNING last_modified::BIGINT
        """
        resource_name = classname(resource)
        placeholders = dict(record_id=record_id,
                            user_id=user_id,
                            resource_name=resource_name)
        inserted = self._execute(query, placeholders=placeholders, commit=True)

        inserted = inserted[0]
        record = {}
        record[resource.modified_field] = inserted['last_modified']
        record[resource.id_field] = record_id
        # XXX inserted['deleted'] = True
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
        filtered_deleted AS (
            SELECT id, last_modified::BIGINT, '{}'::json AS data
              FROM deleted
             WHERE user_id = %%(user_id)s
               AND resource_name = %%(resource_name)s
             %(conditions_deleted)s
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
        placeholders = dict(user_id=user_id, resource_name=resource_name)

        safeholders = defaultdict(six.text_type)

        if filters:
            conditions = [self._format_condition(resource, f) for f in filters]
            and_conditions = ' AND '.join(conditions)
            safeholders['conditions_filter'] = 'AND %s' % and_conditions

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
            cursor = self._conn.cursor()
            safe = cursor.mogrify('LIMIT %s', (limit,))
            safeholders['pagination_limit'] =  safe

        if include_deleted:
            last_modified_filters = [f for f in filters
                                     if f[0] == resource.modified_field]
            if last_modified_filters:
                condition = self._format_condition(resource,
                                                   last_modified_filters[0])
                safeholders['conditions_deleted'] = 'WHERE %s' % condition
        else:
            safeholders['conditions_deleted'] = 'LIMIT 0'

        results = self._execute(query % safeholders, placeholders)
        if not results:
            return [], 0

        count_total = results[0]['count_total']

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

            Field name and value are escaped using cursor.mogrify()
        """
        field, value, operator = filter_

        operators = {
            COMPARISON.EQ: '=',
            COMPARISON.NOT: '<>',
        }
        operator = operators.setdefault(operator, operator)

        cursor = self._conn.cursor()
        is_base_field = field in (resource.id_field, resource.modified_field)
        if not is_base_field:
            field = cursor.mogrify("data->>%s", (field,))
            # PostgreSQL compares JSON fields values as strings
            value = '%s' % value

        if field == resource.modified_field:
            field = "%s::BIGINT" % field

        safe = "%s %s %%s" % (field, operator)
        return cursor.mogrify(safe, (value,))

    def _format_sort(self, resource, sort):
        """Format the sort in SQL instruction.

        :note:

            Field name is escaped using cursor.mogrify()
        """
        field, direction = sort
        direction = 'ASC' if direction > 0 else 'DESC'

        is_base_field = field in (resource.id_field, resource.modified_field)
        if not is_base_field:
            cursor = self._conn.cursor()
            field = cursor.mogrify("data->>%s", (field,))

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
