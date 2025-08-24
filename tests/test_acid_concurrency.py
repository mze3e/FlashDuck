import threading
import time

import duckdb
import pytest


def test_atomicity_transaction_rollback(tmp_path):
    db = tmp_path / "atomic.db"
    conn = duckdb.connect(str(db))
    conn.execute("CREATE TABLE items (id INTEGER)")
    conn.begin()
    conn.execute("INSERT INTO items VALUES (1)")
    with pytest.raises(duckdb.ConversionException):
        conn.execute("INSERT INTO items VALUES ('a')")
    conn.rollback()
    count = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    assert count == 0
    conn.close()


def test_isolation_between_transactions(tmp_path):
    db = tmp_path / "isolation.db"
    conn1 = duckdb.connect(str(db))
    conn2 = duckdb.connect(str(db))
    conn1.execute("CREATE TABLE items (id INTEGER)")
    conn1.commit()
    conn1.begin()
    conn1.execute("INSERT INTO items VALUES (1)")
    count_before = conn2.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    assert count_before == 0
    conn1.commit()
    count_after = conn2.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    assert count_after == 1
    conn1.close()
    conn2.close()


def test_durability_after_commit(tmp_path):
    db = tmp_path / "durability.db"
    conn = duckdb.connect(str(db))
    conn.execute("CREATE TABLE items (id INTEGER)")
    conn.execute("INSERT INTO items VALUES (1)")
    conn.commit()
    conn.close()
    conn2 = duckdb.connect(str(db))
    count = conn2.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    assert count == 1
    conn2.close()


def test_concurrent_read_write(tmp_path):
    db = tmp_path / "concurrent.db"
    conn_init = duckdb.connect(str(db))
    conn_init.execute("CREATE TABLE items (id INTEGER)")
    conn_init.commit()
    conn_init.close()

    total_rows = 50
    done = threading.Event()

    def writer():
        conn_w = duckdb.connect(str(db))
        for i in range(total_rows):
            conn_w.execute("INSERT INTO items VALUES (?)", (i,))
            conn_w.commit()
            time.sleep(0.005)
        conn_w.close()
        done.set()

    read_counts = []

    def reader():
        conn_r = duckdb.connect(str(db))
        while not done.is_set():
            read_counts.append(conn_r.execute("SELECT COUNT(*) FROM items").fetchone()[0])
            time.sleep(0.005)
        conn_r.close()

    t_writer = threading.Thread(target=writer)
    t_reader = threading.Thread(target=reader)
    t_writer.start()
    t_reader.start()
    t_writer.join()
    done.set()
    t_reader.join()

    conn_v = duckdb.connect(str(db))
    final_count = conn_v.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    conn_v.close()
    assert final_count == total_rows
    assert read_counts and read_counts[-1] == total_rows
