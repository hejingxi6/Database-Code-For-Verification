# generate_data.py

import random
import string
import datetime
import mysql.connector

#配置
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "digital_products_platform"
}

N_USERS = 2000
N_PUBLISHERS = 100
N_PRODUCTS = 3000
N_ORDERS = 50000
MAX_ORDER_LINES_PER_ORDER = 5

RANDOM_SEED = 42


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def random_string(n=8):
    return ''.join(random.choices(string.ascii_letters, k=n))


def random_phone():
    return f"+1{random.randint(2000000000, 9999999999)}"


def random_date(start_year=2018, end_year=2025):
    start = datetime.date(start_year, 1, 1).toordinal()
    end = datetime.date(end_year, 12, 31).toordinal()
    day = random.randint(start, end)
    return datetime.date.fromordinal(day)


def random_datetime(start_year=2019, end_year=2025):
    d = random_date(start_year, end_year)
    t = datetime.time(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59)
    )
    return datetime.datetime.combine(d, t)


#基础表

def create_users(conn):
    print("Inserting users...")
    cur = conn.cursor()
    sql = """
        INSERT INTO `User` (User_Name, Email, `Password`, Phone)
        VALUES (%s, %s, %s, %s)
    """
    for i in range(N_USERS):
        # 显式保证唯一性：user0@example.com, user1@example.com, ...
        name = f"user_{i}_{random_string(4)}"
        email = f"user{i}@example.com"
        pwd = random_string(12)
        phone = random_phone()
        cur.execute(sql, (name, email, pwd, phone))
        if i % 1000 == 0:
            conn.commit()
            print(f"  ... {i} users inserted")
    conn.commit()
    cur.close()
    print(f"Inserted {N_USERS} users.")


def create_publishers(conn):
    print("Inserting publishers...")
    cur = conn.cursor()
    sql = """
        INSERT INTO Publisher (Publisher_Name, Email, Contact_Number,
                               Contact, Region, Join_Date, Description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    regions = ["US", "EU", "Asia", "Global"]
    for i in range(N_PUBLISHERS):
        # Publisher_Name 也有唯一约束，加入序号避免重复
        name = f"Publisher_{i}_{random_string(3)}"
        email = f"pub{i}@publisher.com"
        phone = random_phone()
        contact = f"Contact_{random_string(4)}"
        region = random.choice(regions)
        join_date = random_date(2015, 2024)
        desc = "Synthetic publisher data."
        cur.execute(sql, (name, email, phone, contact, region, join_date, desc))
    conn.commit()
    cur.close()
    print(f"Inserted {N_PUBLISHERS} publishers.")


def create_categories(conn):
    print("Inserting categories...")
    categories = [
        "Action Games", "RPG", "Simulation", "Strategy",
        "Music", "Movie", "Productivity Software", "E-book"
    ]
    cur = conn.cursor()
    sql = "INSERT INTO Category (Category_Name) VALUES (%s)"
    for c in categories:
        cur.execute(sql, (c,))
    conn.commit()
    cur.close()
    print(f"Inserted {len(categories)} categories.")


def create_tags(conn):
    print("Inserting tags...")
    tags = [
        "Multiplayer", "Singleplayer", "Co-op", "Indie",
        "AAA", "Casual", "Hardcore", "OpenWorld",
        "SciFi", "Fantasy", "Horror", "Family"
    ]
    cur = conn.cursor()
    sql = "INSERT INTO Tag (Tag_Name) VALUES (%s)"
    for t in tags:
        cur.execute(sql, (t,))
    conn.commit()
    cur.close()
    print(f"Inserted {len(tags)} tags.")


def create_languages(conn):
    print("Inserting languages...")
    langs = ["English", "Chinese", "Japanese", "Korean", "Spanish", "French", "German"]
    cur = conn.cursor()
    sql = "INSERT INTO Language (Language_Name) VALUES (%s)"
    for l in langs:
        cur.execute(sql, (l,))
    conn.commit()
    cur.close()
    print(f"Inserted {len(langs)} languages.")


#工具函数

def get_id_list(cur, table, id_column):
    cur.execute(f"SELECT {id_column} FROM {table}")
    return [row[0] for row in cur.fetchall()]


#Product & 关联

def create_products(conn):
    print("Inserting products...")
    cur = conn.cursor()
    category_ids = get_id_list(cur, "Category", "Category_ID")
    publisher_ids = get_id_list(cur, "Publisher", "Publisher_ID")

    sql = """
        INSERT INTO Product
        (Product_Name, Product_Price, Release_Date, Region, Description,
         Category_ID, Publisher_ID)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    regions = ["Global", "US", "EU", "Asia"]
    for i in range(N_PRODUCTS):
        name = f"Product_{i}_{random_string(5)}"
        price = round(random.uniform(2, 80), 2)
        rdate = random_date(2015, 2025)
        region = random.choice(regions)
        desc = "Synthetic digital product."
        cat = random.choice(category_ids)
        pub = random.choice(publisher_ids)
        cur.execute(sql, (name, price, rdate, region, desc, cat, pub))
        if i % 1000 == 0:
            conn.commit()
            print(f"  ... {i} products inserted")
    conn.commit()
    cur.close()
    print(f"Inserted {N_PRODUCTS} products.")


def create_product_tags(conn):
    print("Linking products and tags...")
    cur = conn.cursor()
    product_ids = get_id_list(cur, "Product", "Product_ID")
    tag_ids = get_id_list(cur, "Tag", "Tag_ID")

    sql = "INSERT IGNORE INTO Product_Tag (Product_ID, Tag_ID) VALUES (%s, %s)"
    for pid in product_ids:
        for _ in range(random.randint(1, 4)):
            tid = random.choice(tag_ids)
            cur.execute(sql, (pid, tid))
    conn.commit()
    cur.close()
    print("Product_Tag links generated.")


def create_product_languages(conn):
    print("Linking products and languages...")
    cur = conn.cursor()
    product_ids = get_id_list(cur, "Product", "Product_ID")
    cur.execute("SELECT Language_Name FROM Language")
    langs = [row[0] for row in cur.fetchall()]

    sql = "INSERT IGNORE INTO Product_Language (Product_ID, Language_Name) VALUES (%s, %s)"
    for pid in product_ids:
        for _ in range(random.randint(1, 3)):
            lang = random.choice(langs)
            cur.execute(sql, (pid, lang))
    conn.commit()
    cur.close()
    print("Product_Language links generated.")


#Orders / Payments / Licenses / Reviews

def create_orders_payments_licenses(conn):
    print("Inserting payments, orders, order_lines, licenses...")
    cur = conn.cursor()

    # 拿到用户和商品列表
    user_ids = get_id_list(cur, "`User`", "User_Acct")
    product_ids = get_id_list(cur, "Product", "Product_ID")

    payment_sql = """
        INSERT INTO Payment (Payment_Method, Amount, `Date`)
        VALUES (%s, %s, %s)
    """
    order_sql = """
        INSERT INTO `Order`
        (User_Acct, Payment_ID, Order_Status, Order_Date)
        VALUES (%s, %s, %s, %s)
    """
    orderline_sql = """
        INSERT INTO Order_Line
        (Order_ID, Line_ID, Product_ID, Unit_Price, Quantity)
        VALUES (%s, %s, %s, %s, %s)
    """
    license_sql = """
        INSERT INTO License
        (License_Code, User_Acct, Product_ID, Access_Link)
        VALUES (%s, %s, %s, %s)
    """

    for i in range(N_ORDERS):
        user = random.choice(user_ids)
        order_date = random_datetime(2020, 2025)
        method = random.choice(["CreditCard", "PayPal", "GiftCard"])

        # 1) 先决定本订单要买哪些商品 & 数量，并算出总价
        n_lines = random.randint(1, MAX_ORDER_LINES_PER_ORDER)
        # 防止同一订单中重复选到同一 product_id，这里用 sample
        chosen_products = random.sample(product_ids, k=n_lines)

        line_items = []
        total = 0.0

        for pid in chosen_products:
            cur.execute("SELECT Product_Price FROM Product WHERE Product_ID = %s", (pid,))
            row = cur.fetchone()
            if not row:
                continue
            price = float(row[0])
            qty = random.randint(1, 3)
            line_total = price * qty
            total += line_total
            line_items.append((pid, price, qty))

        # 理论上 total 一定 >0，如果出意外（比如没查到价格），跳过该订单避免违反 CHECK
        if total <= 0 or not line_items:
            continue

        pay_date = order_date + datetime.timedelta(seconds=random.randint(1, 600))

        # 2) 插入 Payment（直接用正确的 Amount，满足 CHECK (Amount > 0)）
        cur.execute(payment_sql, (method, round(total, 2), pay_date))
        payment_id = cur.lastrowid

        # 3) 插入 Order（关联到该 Payment）
        status = "PAID"
        cur.execute(order_sql, (user, payment_id, status, order_date))
        order_id = cur.lastrowid

        # 4) 插入 Order_Line 和 License
        line_id = 1
        for pid, price, qty in line_items:
            cur.execute(
                orderline_sql,
                (order_id, line_id, pid, price, qty)
            )

            license_code = f"LIC-{order_id}-{line_id}-{random_string(6)}"
            link = f"https://download.example.com/{pid}/{license_code}"
            cur.execute(
                license_sql,
                (license_code, user, pid, link)
            )

            line_id += 1

        if i % 1000 == 0:
            conn.commit()
            print(f"  ... {i} orders generated")

    conn.commit()
    cur.close()
    print(f"Inserted up to {N_ORDERS} orders with payments, order_lines and licenses.")


def create_reviews(conn):
    print("Inserting reviews...")
    cur = conn.cursor()
    user_ids = get_id_list(cur, "`User`", "User_Acct")
    product_ids = get_id_list(cur, "Product", "Product_ID")

    sql = """
        INSERT IGNORE INTO Review
        (User_Acct, Product_ID, Rating, Content)
        VALUES (%s, %s, %s, %s)
    """
    # 大概 2 * N_PRODUCTS 条尝试，IGNORE 保证不会违反 (user, product) 唯一约束
    for _ in range(N_PRODUCTS * 2):
        uid = random.choice(user_ids)
        pid = random.choice(product_ids)
        rating = random.randint(3, 5)
        content = f"Review for product {pid}, rating {rating}."
        cur.execute(sql, (uid, pid, rating, content))

    conn.commit()
    cur.close()
    print("Reviews inserted.")


#主流程

def main():
    random.seed(RANDOM_SEED)
    conn = get_connection()

    create_users(conn)
    create_publishers(conn)
    create_categories(conn)
    create_tags(conn)
    create_languages(conn)
    create_products(conn)
    create_product_tags(conn)
    create_product_languages(conn)
    create_orders_payments_licenses(conn)
    create_reviews(conn)

    conn.close()
    print("Data generation completed successfully.")


if __name__ == "__main__":
    main()
