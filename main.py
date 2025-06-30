import os
from data_validator import valid_order_rows_flexible
from db_manager import (
    setup_database,
    run_aggregations,
    count_invalid_items,
    get_invalid_items
)
from json_exporter import export_json

CSV_PATH = 'user_data.csv'
DB_PATH = 'pepper_orders.db'


def main():
    print("=== Pepper Onboarding Pipeline ===")

    # Validate CSV
    if not os.path.exists(CSV_PATH):
        print(f" Error: {CSV_PATH} not found.")
        return

    print(f" Reading and validating data from {CSV_PATH}...")
    validated_data = list(valid_order_rows_flexible(CSV_PATH))
    print(f"Rows processed: {len(validated_data)}")

    # Load to database
    conn, cursor = setup_database(DB_PATH, validated_data, overwrite=False)
    print(f"Database updated at {DB_PATH}")

    # Show invalid, active rows for review
    invalid_count = count_invalid_items(cursor)
    print(f"\n Invalid rows needing review: {invalid_count}")

    if invalid_count > 0:
        print("\nDetails of invalid rows:")
        for row in get_invalid_items(cursor):
            print(f"- Order ID: {row[0]}, Item: {row[1]}, Quantity: {row[2]}, Unit Price: {row[3]}, Error: {row[4]}")

    # Business Analytics
    print("\nOrder Analytics:")
    orders, top_customer, item_counts = run_aggregations(cursor)

    print("\n Total Value Per Order:")
    for order in orders:
        print(f"- Order ID: {order[0]}, Customer ID: {order[1]}, Total Value: ${order[2]:.2f}")

    if top_customer:
        print(f"\n Top Customer: {top_customer[0]} with Total Spend: ${top_customer[1]:.2f}")

    print("\nUnique Items Per Order:")
    for item in item_counts:
        print(f"- Order ID: {item[0]}, Unique Items: {item[1]}")

    # JSON Export
    print("\nExported JSON (Valid & Active Orders):")
    json_output = export_json(cursor)
    print(json_output)

    # Save JSON data to a file
    with open('exported_orders.json', 'w') as f:
        f.write(json_output)
    print("\nJSON saved to exported_orders.json")

    conn.close()


if __name__ == "__main__":
    main()
