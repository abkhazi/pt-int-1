import pytest
import mysql.connector
from time import time

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

@pytest.fixture(scope="function", autouse=True)
def reset_database(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("DELETE FROM test_table;")
    db_connection.commit()

def rop_index_if_exists_idx_str(db_connection):
    cursor = db_connection.cursor()

    drop_index_if_exists(cursor, "idx_str", "test_table")

def drop_index_if_exists(cursor, index_name, table_name):
    cursor.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name='{index_name}';")
    result = cursor.fetchone()
    if result:
        cursor.execute(f"DROP INDEX {index_name} ON {table_name};")

def populate_table(cursor, db_connection):
    cursor.execute("DELETE FROM test_table;")
    cursor.execute("ALTER TABLE test_table AUTO_INCREMENT = 1;")
    values = [(f'employee_{i}', f'department_{i % 10 + 1}') for i in range(1, 100001)]
    cursor.executemany("INSERT INTO test_table (employee, department) VALUES (%s, %s);", values)
    db_connection.commit()

def test_performance_select_like(db_connection):
    cursor = db_connection.cursor()

    # Populate table with data
    populate_table(cursor, db_connection)

    # Without index
    drop_index_if_exists(cursor, "idx_employee", "test_table")
    start_time_without_index = time()
    cursor.execute("EXPLAIN SELECT SQL_NO_CACHE * FROM test_table WHERE employee LIKE 'employee_%';")
    result_without_index = cursor.fetchall()
    time_without_index = time() - start_time_without_index

    # With index
    drop_index_if_exists(cursor, "idx_employee", "test_table")
    cursor.execute("CREATE INDEX idx_employee ON test_table(employee);")
    start_time_with_index = time()
    cursor.execute("EXPLAIN SELECT SQL_NO_CACHE * FROM test_table WHERE employee LIKE 'employee_%';")
    result_with_index = cursor.fetchall()
    time_with_index = time() - start_time_with_index

    # Output the execution time to the console
    print(f"Time without index: {time_without_index:.6f} seconds")
    print(f"Time with index: {time_with_index:.6f} seconds")

    assert time_with_index < time_without_index

    print("EXPLAIN result with index:", result_with_index)
    print("EXPLAIN result without index:", result_without_index)

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
