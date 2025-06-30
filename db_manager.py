import sqlite3


def setup_database(db_path, validated_data, overwrite=False):
    """
    Initializes the SQLite database and inserts validated ERP order data.

    Modes:
    - overwrite=True: clears existing data and loads fresh CSV
    - overwrite=False: incrementally upserts new data and deletes removed rows
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    _create_main_tables(cursor)

    if overwrite:
        _overwrite_load(cursor, validated_data)
    else:
        _incremental_sync(cursor, validated_data)

    conn.commit()
    return conn, cursor


def _create_main_tables(cursor):
    """
    Creates the main production tables if they don't exist.
    Ensures schema for orders and order_items with correct constraints.
    """
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER NOT NULL PRIMARY KEY,
            customer_id INTEGER,
            order_date DATE
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            item TEXT,
            quantity INTEGER,
            unit_price REAL,
            is_valid BOOLEAN,
            error_message TEXT,
            UNIQUE(order_id, item),
            FOREIGN KEY(order_id) REFERENCES orders(order_id)
        )
    ''')


def _overwrite_load(cursor, validated_data):
    """
    Clears all existing production data and loads new CSV snapshot.
    Upserts all rows, handling duplicate (order_id, item) in CSV automatically.
    """
    print("Overwrite mode: Clearing existing data.")
    cursor.execute("DELETE FROM order_items")
    cursor.execute("DELETE FROM orders")

    for row in validated_data:
        _upsert_order(cursor, row)
        _upsert_order_item(cursor, row)


def _incremental_sync(cursor, validated_data):
    """
    Performs incremental sync:
    - Loads new CSV into staging table
    - Upserts changes into production
    - Deletes any items missing from the new snapshot
    """
    print("Sync mode: Incremental upsert with deletes of missing rows.")

    # 1. Clear and create staging table
    cursor.execute('DROP TABLE IF EXISTS staging_order_items')
    cursor.execute('''
        CREATE TABLE staging_order_items (
            order_id INTEGER NOT NULL,
            item TEXT,
            quantity INTEGER,
            unit_price REAL,
            is_valid BOOLEAN,
            error_message TEXT,
            customer_id INTEGER,
            order_date DATE,
            PRIMARY KEY(order_id, item)
        )
    ''')

    # 2. Load new CSV data into staging
    for row in validated_data:
        cursor.execute('''
            INSERT INTO staging_order_items (
                order_id, item, quantity, unit_price, is_valid, error_message, customer_id, order_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id, item) DO UPDATE SET
                quantity=excluded.quantity,
                unit_price=excluded.unit_price,
                is_valid=excluded.is_valid,
                error_message=excluded.error_message,
                customer_id=excluded.customer_id,
                order_date=excluded.order_date
        ''', (
            row['order_id'], row['item'], row['quantity'], row['unit_price'],
            row['is_valid'], row['error_message'], row['customer_id'], row['date']
        ))
    cursor.connection.commit()

    # 3. Upsert staging data into production tables
    cursor.execute('SELECT * FROM staging_order_items')
    for row in cursor.fetchall():
        (order_id, item, quantity, unit_price, is_valid, error_message, customer_id, order_date) = row

        _upsert_order(cursor, {
            'order_id': order_id,
            'customer_id': customer_id,
            'date': order_date
        })
        _upsert_order_item(cursor, {
            'order_id': order_id,
            'item': item,
            'quantity': quantity,
            'unit_price': unit_price,
            'is_valid': is_valid,
            'error_message': error_message
        })

    # 4. Delete order_items missing from staging
    cursor.execute('''
        DELETE FROM order_items
        WHERE (order_id, item) NOT IN (
            SELECT order_id, item FROM staging_order_items
        )
    ''')

    # 5. Delete orders with no remaining items
    cursor.execute('''
        DELETE FROM orders
        WHERE order_id NOT IN (
            SELECT DISTINCT order_id FROM order_items
        )
    ''')

    # 6. Clean up staging table
    cursor.execute('DELETE FROM staging_order_items')


def _upsert_order(cursor, row):
    """
    Inserts or updates an order in the orders table.
    Ensures customer_id and date are kept current.
    """
    cursor.execute('''
        INSERT INTO orders (order_id, customer_id, order_date)
        VALUES (?, ?, ?)
        ON CONFLICT(order_id) DO UPDATE SET
            customer_id=excluded.customer_id,
            order_date=excluded.order_date
    ''', (row['order_id'], row.get('customer_id'), row.get('date')))


def _upsert_order_item(cursor, row):
    """
    Inserts or updates an order item in the order_items table.
    Updates all fields.
    """
    item = row['item'] or "__BLANK_ITEM__"
    cursor.execute('''
        INSERT INTO order_items (
            order_id, item, quantity, unit_price, is_valid, error_message
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(order_id, item) DO UPDATE SET
            quantity=excluded.quantity,
            unit_price=excluded.unit_price,
            is_valid=excluded.is_valid,
            error_message=excluded.error_message
    ''', (
        row['order_id'], item, row['quantity'], row['unit_price'],
        row['is_valid'], row['error_message']
    ))


def run_aggregations(cursor):
    """
    Executes SQL aggregation queries on current order_items.

    Returns:
        tuple:
            - orders: List of (order_id, customer_id, total_value)
            - top_customer: (customer_id, total_spend)
            - item_counts: List of (order_id, unique_item_count)
    """
    orders = _get_total_order_values(cursor)
    top_customer = _get_top_customer(cursor)
    item_counts = _get_unique_item_counts(cursor)

    return orders, top_customer, item_counts


def _get_total_order_values(cursor):
    """
    Calculates total value per order.
    Includes only items with valid item value, quantity and price.
    """
    cursor.execute('''
        SELECT o.order_id, o.customer_id, SUM(i.quantity * i.unit_price) AS total_value
        FROM orders o
        JOIN order_items i ON o.order_id = i.order_id
        WHERE i.item <> "__BLANK_ITEM__" AND i.quantity IS NOT NULL AND i.unit_price IS NOT NULL
        GROUP BY o.order_id
    ''')
    return cursor.fetchall()


def _get_top_customer(cursor):
    """
    Identifies the top customer by total spend.
    """
    cursor.execute('''
        SELECT o.customer_id, SUM(i.quantity * i.unit_price) AS total_spend
        FROM orders o
        JOIN order_items i ON o.order_id = i.order_id
        WHERE i.quantity IS NOT NULL AND i.unit_price IS NOT NULL
        GROUP BY o.customer_id
        ORDER BY total_spend DESC
        LIMIT 1
    ''')
    return cursor.fetchone()


def _get_unique_item_counts(cursor):
    """
    Counts the number of unique items per order.
    Only includes items with a quantity.
    """
    cursor.execute('''
        SELECT o.order_id, COUNT(DISTINCT i.item) AS unique_items
        FROM orders o
        JOIN order_items i ON o.order_id = i.order_id
        WHERE i.quantity IS NOT NULL
        GROUP BY o.order_id
    ''')
    return cursor.fetchall()


def count_invalid_items(cursor):
    """
    Counts total number of invalid order_items.

    Returns:
        int: Number of invalid records needing review.
    """
    cursor.execute('''
        SELECT COUNT(*)
        FROM order_items
        WHERE is_valid = 0
    ''')
    return cursor.fetchone()[0]


def get_invalid_items(cursor):
    """
    Retrieves details of invalid order_items.

    Returns:
        list of tuples: Each with (order_id, item, quantity, unit_price, error_message)
    """
    cursor.execute('''
        SELECT order_id, item, quantity, unit_price, error_message
        FROM order_items
        WHERE is_valid = 0
    ''')
    return cursor.fetchall()
