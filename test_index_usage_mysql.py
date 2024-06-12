import pytest
import mysql.connector

@pytest.fixture(scope="module")
def db_connection():
    conn = mysql.connector.connect(
        host="localhost",
        user="test_user",
        password="password",
        database="test_db"
    )
    yield conn
    conn.close()

def drop_index_if_exists(cursor, index_name, table_name):
    cursor.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name='{index_name}';")
    result = cursor.fetchone()
    if result:
        cursor.execute(f"DROP INDEX {index_name} ON {table_name};")

def populate_table(cursor, db_connection):
    cursor.execute("DELETE FROM test_table;")
    cursor.execute("ALTER TABLE test_table AUTO_INCREMENT = 1;")
    values = [(f'employee_{i}', f'department_{i % 10 + 1}') for i in range(1, 10001)]
    cursor.executemany("INSERT INTO test_table (employee, department) VALUES (%s, %s);", values)
    db_connection.commit()

def test_index_not_used_with_like(db_connection):
    cursor = db_connection.cursor()

    # Populate table with data
    populate_table(cursor, db_connection)

    # Create index
    drop_index_if_exists(cursor, "idx_str", "test_table")
    cursor.execute("CREATE INDEX idx_str ON test_table(employee);")

    # Example when index is not used (LIKE with wildcard characters)
    cursor.execute("EXPLAIN SELECT * FROM test_table WHERE employee LIKE '%employee%';")
    explain_result = cursor.fetchall()

    # Check if index is not used
    index_used = any(row[1] == 'idx_str' for row in explain_result)
    assert not index_used, "Index was used: %s" % explain_result

    # Output the EXPLAIN result for debugging purposes
    print("EXPLAIN result for LIKE '%employee%':", explain_result)

def test_index_not_used_with_upper(db_connection):
    cursor = db_connection.cursor()

    # Populate table with data
    populate_table(cursor, db_connection)

    # Create index
    drop_index_if_exists(cursor, "idx_str", "test_table")
    cursor.execute("CREATE INDEX idx_str ON test_table(employee);")

    # Example when index is not used (UPPER function on column with LIKE)
    cursor.execute("EXPLAIN SELECT * FROM test_table WHERE UPPER(employee) LIKE UPPER('employee%');")
    explain_result = cursor.fetchall()

    # Check if index is not used
    index_used = any(row[1] == 'idx_str' for row in explain_result)
    assert not index_used, "Index was used: %s" % explain_result

    # Output the EXPLAIN result for debugging purposes
    print("EXPLAIN result for UPPER(employee):", explain_result)
