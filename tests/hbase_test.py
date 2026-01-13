# hbase/test_hbase_functions.py
import happybase
import time


def test_connection():
    """Test 1: Connection to HBase"""
    try:
        connection = happybase.Connection("localhost", port=9090)
        connection.tables()
        connection.close()
        print("✅ Test 1 PASSED: HBase connection successful")
        return True
    except Exception as e:
        print(f"❌ Test 1 FAILED: {e}")
        return False


def test_list_tables():
    """Test 2: List tables"""
    try:
        connection = happybase.Connection("localhost", port=9090)
        tables = [t.decode() for t in connection.tables()]
        connection.close()
        print(f"✅ Test 2 PASSED: Found tables: {tables}")
        return "police_events" in tables
    except Exception as e:
        print(f"❌ Test 2 FAILED: {e}")
        return False


def test_write_record():
    """Test 3: Write a test record"""
    try:
        connection = happybase.Connection("localhost", port=9090)
        table = connection.table("police_events")

        row_key = f"TEST#dr5ru#NYC#999999"
        data = {
            b"common:date_id": b"20250113",
            b"common:latitude": b"40.7128",
            b"common:longitude": b"-74.0060",
            b"common:geohash": b"dr5ru",
            b"common:city_code": b"NYC",
            b"metadata:test": b"true",
        }

        table.put(row_key.encode(), data)
        connection.close()
        print("✅ Test 3 PASSED: Record written")
        return True
    except Exception as e:
        print(f"❌ Test 3 FAILED: {e}")
        return False


def test_read_record():
    """Test 4: Read the test record"""
    try:
        connection = happybase.Connection("localhost", port=9090)
        table = connection.table("police_events")

        row_key = f"TEST#dr5ru#NYC#999999"
        row = table.row(row_key.encode())
        connection.close()

        if row:
            print(f"✅ Test 4 PASSED: Record retrieved: {row}")
            return True
        else:
            print("❌ Test 4 FAILED: Record not found")
            return False
    except Exception as e:
        print(f"❌ Test 4 FAILED: {e}")
        return False


def test_scan_table():
    """Test 5: Scan first 5 records"""
    try:
        connection = happybase.Connection("localhost", port=9090)
        table = connection.table("police_events")

        count = 0
        for key, data in table.scan(limit=5):
            count += 1
            print(f"  Row: {key.decode()}")

        connection.close()
        print(f"✅ Test 5 PASSED: Scanned {count} records")
        return True
    except Exception as e:
        print(f"❌ Test 5 FAILED: {e}")
        return False


def test_delete_record():
    """Test 6: Delete test record (cleanup)"""
    try:
        connection = happybase.Connection("localhost", port=9090)
        table = connection.table("police_events")

        row_key = f"TEST#dr5ru#NYC#999999"
        table.delete(row_key.encode())
        connection.close()
        print("✅ Test 6 PASSED: Test record deleted")
        return True
    except Exception as e:
        print(f"❌ Test 6 FAILED: {e}")
        return False


if __name__ == "__main__":
    print("🧪 Running HBase Functional Tests\n")

    tests = [
        test_connection,
        test_list_tables,
        test_write_record,
        test_read_record,
        test_scan_table,
        test_delete_record,
    ]

    passed = 0
    for test in tests:
        if test():
            passed += 1
        print()

    print(f"\n📊 Results: {passed}/{len(tests)} tests passed")
