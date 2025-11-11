import time
import random
import statistics
import mysql.connector
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "digital_products_platform"
}

RANDOM_SEED = 123


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def percentile(data, p):
    if not data:
        return None
    data_sorted = sorted(data)
    k = int(round((p / 100) * (len(data_sorted) - 1)))
    return data_sorted[k]


#SELECT 查询性能测试

def pick_random_ids(cur, table, col, limit=1000):
    cur.execute(f"SELECT {col} FROM {table} ORDER BY {col} LIMIT {limit}")
    return [r[0] for r in cur.fetchall()]


def test_select_queries(conn, loops=200):
    print("\n[TEST] SELECT query performance")

    cur = conn.cursor()
    product_ids = pick_random_ids(cur, "Product", "Product_ID")
    user_ids = pick_random_ids(cur, "`User`", "User_Acct")
    cur.close()

    latencies = []

    q1 = """
        SELECT p.Product_ID, p.Product_Name, p.Product_Price,
               c.Category_Name, pub.Publisher_Name
        FROM Product p
        JOIN Category c ON p.Category_ID = c.Category_ID
        JOIN Publisher pub ON p.Publisher_ID = pub.Publisher_ID
        WHERE p.Product_ID = %s
    """

    q2 = """
        SELECT o.Order_ID, o.Order_Date, pay.Payment_Method, pay.Amount
        FROM `Order` o
        LEFT JOIN Payment pay ON o.Payment_ID = pay.Payment_ID
        WHERE o.User_Acct = %s
        ORDER BY o.Order_Date DESC
        LIMIT 20
    """

    q3 = """
        SELECT ol.Product_ID, COUNT(*) AS sales
        FROM Order_Line ol
        GROUP BY ol.Product_ID
        ORDER BY sales DESC
        LIMIT 20
    """

    cur = conn.cursor(dictionary=True)

    for _ in range(loops):
        # Q1
        pid = random.choice(product_ids)
        t0 = time.perf_counter()
        cur.execute(q1, (pid,))
        cur.fetchall()
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000)

        # Q2
        uid = random.choice(user_ids)
        t0 = time.perf_counter()
        cur.execute(q2, (uid,))
        cur.fetchall()
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000)

        # Q3
        t0 = time.perf_counter()
        cur.execute(q3)
        cur.fetchall()
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000)

    cur.close()

    avg_ms = statistics.mean(latencies)
    max_ms = max(latencies)
    p95_ms = percentile(latencies, 95)

    print(f"Total queries: {len(latencies)}")
    print(f"Average latency: {avg_ms:.2f} ms")
    print(f"95th percentile: {p95_ms:.2f} ms")
    print(f"Max latency: {max_ms:.2f} ms")

    return {
        "avg_ms": avg_ms,
        "p95_ms": p95_ms,
        "max_ms": max_ms
    }


#2. 写事务测试

def create_single_order_transaction(conn, user_id, product_ids):
    cur = conn.cursor()
    try:
        # 选择本次订单的商品组合
        n = random.randint(1, 4)
        chosen = random.sample(product_ids, n)
        line_items = []
        total = 0.0

        order_date = datetime.now()

        for pid in chosen:
            cur.execute(
                "SELECT Product_Price FROM Product WHERE Product_ID = %s",
                (pid,)
            )
            row = cur.fetchone()
            if not row:
                continue
            price = float(row[0])
            qty = random.randint(1, 3)
            total += price * qty
            line_items.append((pid, price, qty))

        # 如果没成功选到商品，或总价为0，直接放弃这个事务
        if total <= 0 or not line_items:
            cur.close()
            return False

        method = random.choice(["CreditCard", "PayPal", "GiftCard"])
        pay_date = order_date + timedelta(seconds=random.randint(1, 600))

        # 开启显式事务（此时 autocommit=False 由上层保证）
        # 1) 插入 Payment（直接用正确金额，满足 CHECK）
        cur.execute(
            "INSERT INTO Payment (Payment_Method, Amount, `Date`) "
            "VALUES (%s, %s, %s)",
            (method, round(total, 2), pay_date)
        )
        payment_id = cur.lastrowid

        # 2) 插入 Order
        cur.execute(
            "INSERT INTO `Order` "
            "(User_Acct, Payment_ID, Order_Status, Order_Date) "
            "VALUES (%s, %s, %s, %s)",
            (user_id, payment_id, "PAID", order_date)
        )
        order_id = cur.lastrowid

        # 3) 插入 Order_Line & License
        line_id = 1
        for pid, price, qty in line_items:
            cur.execute(
                "INSERT INTO Order_Line "
                "(Order_ID, Line_ID, Product_ID, Unit_Price, Quantity) "
                "VALUES (%s, %s, %s, %s, %s)",
                (order_id, line_id, pid, price, qty)
            )

            license_code = f"TX-{order_id}-{line_id}-{random.randint(1000, 9999)}"
            link = f"https://download.example.com/{pid}/{license_code}"
            cur.execute(
                "INSERT INTO License "
                "(License_Code, User_Acct, Product_ID, Access_Link) "
                "VALUES (%s, %s, %s, %s)",
                (license_code, user_id, pid, link)
            )

            line_id += 1

        # 全部成功则提交
        conn.commit()
        cur.close()
        return True

    except Exception as e:
        conn.rollback()
        cur.close()
        print("Transaction failed:", e)
        return False


def test_write_transactions(conn, loops=100):
    print("\n[TEST] Transactional write performance")

    cur = conn.cursor()
    cur.execute("SELECT User_Acct FROM `User` ORDER BY User_Acct LIMIT 1000")
    user_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT Product_ID FROM Product ORDER BY Product_ID LIMIT 2000")
    product_ids = [r[0] for r in cur.fetchall()]
    cur.close()

    # 使用手动事务控制，避免 nested transaction 问题
    conn.autocommit = False

    latencies = []
    success = 0
    failures = 0

    for _ in range(loops):
        uid = random.choice(user_ids)
        t0 = time.perf_counter()
        ok = create_single_order_transaction(conn, uid, product_ids)
        t1 = time.perf_counter()

        if ok:
            success += 1
        else:
            failures += 1

        latencies.append((t1 - t0) * 1000)

    # 恢复 autocommit，避免影响后续查询
    conn.autocommit = True

    avg_ms = statistics.mean(latencies)
    max_ms = max(latencies)
    p95_ms = percentile(latencies, 95)

    # 粗略 TPS：成功事务数 / 所有事务耗时总和
    total_time_sec = sum(lat / 1000.0 for lat in latencies)
    approx_tps = (success / total_time_sec) if total_time_sec > 0 else 0.0

    print(f"Total transactions: {loops}")
    print(f"Success: {success}, Failures: {failures}")
    print(f"Average latency per tx: {avg_ms:.2f} ms")
    print(f"95th percentile: {p95_ms:.2f} ms")
    print(f"Max latency: {max_ms:.2f} ms")
    print(f"Approx throughput: {approx_tps:.2f} tx/s")

    return {
        "success": success,
        "failures": failures,
        "avg_ms": avg_ms,
        "p95_ms": p95_ms,
        "max_ms": max_ms,
        "approx_tps": approx_tps
    }


#简单一致性检查

def consistency_checks(conn):
    print("\n[TEST] Basic consistency checks")
    cur = conn.cursor()

    # 已支付订单是否都有合法 Payment
    cur.execute("""
        SELECT COUNT(*) FROM `Order` o
        WHERE o.Order_Status = 'PAID'
          AND (o.Payment_ID IS NULL
               OR o.Payment_ID NOT IN (SELECT Payment_ID FROM Payment))
    """)
    missing_pay = cur.fetchone()[0]

    # 是否有孤儿 Order_Line
    cur.execute("""
        SELECT COUNT(*) FROM Order_Line ol
        LEFT JOIN `Order` o ON ol.Order_ID = o.Order_ID
        WHERE o.Order_ID IS NULL
    """)
    orphan_ol = cur.fetchone()[0]

    # 是否有孤儿 License
    cur.execute("""
        SELECT COUNT(*) FROM License l
        LEFT JOIN `User` u ON l.User_Acct = u.User_Acct
        LEFT JOIN Product p ON l.Product_ID = p.Product_ID
        WHERE u.User_Acct IS NULL OR p.Product_ID IS NULL
    """)
    orphan_license = cur.fetchone()[0]

    cur.close()

    print(f"Paid orders without valid payment: {missing_pay}")
    print(f"Orphan order lines (no order): {orphan_ol}")
    print(f"Orphan licenses (no user/product): {orphan_license}")

    return {
        "missing_payment_for_paid_order": missing_pay,
        "orphan_order_lines": orphan_ol,
        "orphan_licenses": orphan_license
    }


# ========== 主函数 ==========

def main():
    random.seed(RANDOM_SEED)
    conn = get_connection()

    select_stats = test_select_queries(conn, loops=200)
    tx_stats = test_write_transactions(conn, loops=100)
    cons_stats = consistency_checks(conn)

    conn.close()

    print("\n=== SUMMARY (for report) ===")
    print(select_stats)
    print(tx_stats)
    print(cons_stats)


if __name__ == "__main__":
    main()
