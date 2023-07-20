#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Script para migrar la estructura y los datos de una base de datos a otra."""


import json
import psycopg2
from datetime import datetime
from decimal import Decimal

SOURCE_HOST = 'localhost'
SOURCE_PORT = 5432
SOURCE_DB = 'demo'
SOURCE_USER = 'postgres'
SOURCE_PASS = 'postgres'
SOURCE_SCHEMA = 'bookings'

TARGET_HOST = 'localhost'
TARGET_PORT = 5432
TARGET_DB = 'demo'
TARGET_USER = 'postgres'
TARGET_PASS = 'postgres'
TARGET_SCHEMA = 'bookings_new'

tables_to_migrate = [
    'aircrafts_data',
    'airports_data',
    'boarding_passes',
    'bookings',
    'flights',
    'seats',
    'ticket_flights',
    'tickets',
]


def migrate_table_structure(source_cursor, source_table_name, target_cursor, target_schema, target_table_name):
    """Replica la estructura de la tabla en la base de datos de destino."""

    # Verificamos si la tabla existe en la base de datos destino
    target_cursor.execute(
        f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '{target_schema}' and table_name = '{target_table_name}');"
    )
    if target_cursor.fetchone()[0]:
        raise ValueError(f'La tabla {target_table_name} ya existe en la base de datos de destino.')

    source_cursor.execute(
        f"SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '{source_table_name}' ORDER BY ordinal_position;"
    )
    table_structure = source_cursor.fetchall()

    # Creamos la tabla en destino
    sql_create_table = f'CREATE TABLE {target_schema}.{target_table_name} ('
    for column_name, data_type, character_maximum_length in table_structure:
        sql_create_table += f'{column_name} {data_type}'
        if character_maximum_length:
            sql_create_table += f'({character_maximum_length})'
        sql_create_table += ', '
    sql_create_table = sql_create_table[:-2]  # <- Eliminamos la última coma y el espacio
    sql_create_table += ')'

    target_cursor.execute(sql_create_table)


def migrate_constraints(source_cursor, source_table_name, target_cursor, target_schema, target_table_name, prefix):
    """Replica las restricciones, incluyendo las foreign keys de la tabla en la base de datos de destino."""

    # Obtenemos las restricciones de la tabla origen
    source_cursor.execute(
        f"SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = '{source_table_name}'::regclass"
    )
    restricciones = source_cursor.fetchall()

    # Creamos las restricciones en la base de datos destino
    for restriccion in restricciones:
        conname, constraint_def = restriccion
        conname = conname.lower()
        constraint_def = constraint_def.replace(
            f'{source_table_name} ', f'{target_table_name} {target_table_name.lower()} '
        )
        target_cursor.execute(
            f'ALTER TABLE {target_schema}.{target_table_name} ADD CONSTRAINT {prefix}{conname} {constraint_def}'
        )


def migrates_sequences(
    source_cursor, source_schema, source_table_name, target_cursor, target_schema, target_table_name, prefix
):
    """Replica los secuenciadores de la tabla en la base de datos de destino."""

    # Obtenemos la secuencia de la tabla origen
    source_cursor.execute(
        f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{source_schema}' AND table_name = '{source_table_name}' AND column_default LIKE 'nextval%'"
    )
    columnas_secuencia = source_cursor.fetchall()
    if columnas_secuencia:
        columna_secuencia = columnas_secuencia[0][0]
        seq_name = f'{prefix}{target_schema}.{target_table_name}_{columna_secuencia}_seq'

        # Creamos la secuencia en la base de datos destino
        target_cursor.execute(f'CREATE SEQUENCE {seq_name};')
        sql_sequence = (
            f"SELECT setval('{seq_name}', (SELECT max({columna_secuencia}) FROM {target_schema}.{target_table_name}))"
        )
        target_cursor.execute(sql_sequence)


# TODO: Hacer pruebas con otros tipos de datos
def migrate_data(source_cursor, source_schema, source_table_name, target_cursor, target_schema, target_table_name):
    """Copia los registros de la tabla de origen a la tabla de destino."""

    source_cursor.execute(f'SELECT * FROM {source_schema}.{source_table_name};')
    rows = source_cursor.fetchall()

    for row in rows:
        values = []
        for value in row:

            if isinstance(value, float) or isinstance(value, int) or isinstance(value, Decimal):
                value = str(value)

            if isinstance(value, str) or isinstance(value, datetime):
                value = f"'{value}'"

            if isinstance(value, dict):
                value = f"'{json.dumps(value)}'"

            if value is None:
                value = 'null'

            values.append(value)

        data = ', '.join(values)
        sql_insert = f'INSERT INTO {target_schema}.{target_table_name} VALUES ({data});'
        target_cursor.execute(sql_insert)


# Conexión a la base de datos de origen
source_conn = psycopg2.connect(
    host=SOURCE_HOST, database=SOURCE_DB, port=SOURCE_PORT, user=SOURCE_USER, password=SOURCE_PASS
)
_source_cursor = source_conn.cursor()

# Conexión a la base de datos de destino
target_conn = psycopg2.connect(
    host=TARGET_HOST, database=TARGET_DB, port=TARGET_PORT, user=TARGET_USER, password=TARGET_PASS
)
_target_cursor = target_conn.cursor()

PREFIX = ''

tables_cant = len(tables_to_migrate)

try:
    for i, _source_table_name in enumerate(tables_to_migrate):
        _target_table_name = f'{PREFIX}{_source_table_name}'
        print(f'Migrando la tabla {_source_table_name}...')
        progress = (i + 1) / tables_cant * 100
        print(f'Procesando {i + 1}/{tables_cant} ({progress:.2f}%)', end='\r')

        _target_cursor.execute('BEGIN;')
        migrate_table_structure(_source_cursor, _source_table_name, _target_cursor, TARGET_SCHEMA, _target_table_name)
        migrate_constraints(
            _source_cursor, _source_table_name, _target_cursor, TARGET_SCHEMA, _target_table_name, PREFIX
        )
        migrates_sequences(
            _source_cursor,
            SOURCE_SCHEMA,
            _source_table_name,
            _target_cursor,
            TARGET_SCHEMA,
            _target_table_name,
            prefix='',
        )

        # Comentado por el momento la importación de datos hasta tanto se resuelva el error con los distintos tipos recibidos
        migrate_data(
            _source_cursor, SOURCE_SCHEMA, _source_table_name, _target_cursor, TARGET_SCHEMA, _target_table_name
        )
        print(f'Tabla {_source_table_name} migrada correctamente.')

        # Hacemos un commit parcial por cada tabla
        _target_cursor.execute('COMMIT;')

except Exception as ex:
    print(f'Error al migrar la tabla {_source_table_name}: {ex}')
    _target_cursor.execute('ROLLBACK;')

# Cerramos las conexiones
_source_cursor.close()
source_conn.close()
_target_cursor.close()
target_conn.close()
