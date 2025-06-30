import json


def export_json(cursor):
    """
    Builds and returns a JSON string representing all ACTIVE order data,
    including invalid items.

    Filters:
    - includes is_valid and error_message for review
    - computes total_price ONLY from valid items

    Args:
        cursor (sqlite3.Cursor): Active cursor connected to the SQLite database.

    Returns:
        str: A JSON-formatted string representing orders and their items.
    """
    # Fetch all orders
    cursor.execute('''
        SELECT order_id, customer_id, order_date
        FROM orders
    ''')
    orders = cursor.fetchall()

    result = []

    for order_id, customer_id, order_date in orders:
        # Fetch *all active* items for this order, valid or invalid
        cursor.execute('''
            SELECT item, quantity, unit_price, is_valid, error_message
            FROM order_items
            WHERE order_id = ?
        ''', (order_id,))
        items = cursor.fetchall()

        if not items:
            continue  # Skip orders with no active items at all

        # Compute total only from items with quantity and price
        total_price = sum(
            (qty * price) for item, qty, price, is_valid, _ in items
            if item != "__BLANK_ITEM__" and qty is not None and price is not None
        )

        # Build JSON object
        record = {
            "order_id": order_id,
            "customer_id": customer_id,
            "date": order_date,
            "total_price": round(total_price, 2),
            "items": [
                {
                    "item": item,
                    "quantity": qty,
                    "unit_price": price,
                    "is_valid": bool(is_valid),
                    "error_message": error
                }
                for item, qty, price, is_valid, error in items
            ]
        }

        result.append(record)

    return json.dumps(result, indent=2)






