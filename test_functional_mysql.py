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

def test_select_like_without_index(db_connection):
    cursor = db_connection.cursor()

    # Populate table with data
    populate_table(cursor, db_connection)  # Передаем db_connection в функцию populate_table

    # Perform SELECT without index
    cursor.execute("SELECT * FROM test_table WHERE employee LIKE 'employee_1%';")
    result_without_index = cursor.fetchall()

    # Create index
    drop_index_if_exists(cursor, "idx_str", "test_table")
    cursor.execute("CREATE INDEX idx_str ON test_table(employee);")

    # Perform SELECT with index
    cursor.execute("SELECT * FROM test_table WHERE employee LIKE 'employee_1%';")

    result_with_index = cursor.fetchall()

    # Ensure results are the same
    assert result_without_index == result_with_index
