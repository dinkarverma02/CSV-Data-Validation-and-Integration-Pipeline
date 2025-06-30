import csv
from datetime import datetime


def valid_order_rows_flexible(file_path):
    """
    Reads and validates ERP-style order data from a CSV file.

    Instead of skipping invalid rows entirely, this version yields
    *all* rows with an 'is_valid' flag and an 'error_message' field
    explaining any validation failures.

    This approach allows storing and reviewing invalid rows later,
    supporting real-world onboarding workflows.

    Args:
        file_path (str): Path to the CSV file containing raw ERP export data.

    Yields:
        dict: A dictionary representing a normalized order row with validation results.
    """
    with open(file_path, newline='') as f:
        # turns each line into a dict
        reader = csv.DictReader(f)

        # Normalize headers
        reader.fieldnames = [h.strip().lower().replace(" ", "_") for h in reader.fieldnames]
        seen_orders = set()

        for row in reader:
            # Normalize row keys
            row = {k.strip().lower().replace(" ", "_"): v for k, v in row.items()}
            result = _validate_row(row)

            # Track duplicates for validated rows
            if result['is_valid']:
                order_key = (result['order_id'], result['item'])
                if order_key in seen_orders:
                    result['is_valid'] = False
                    result['error_message'] = "Duplicate order_id and item"
                else:
                    seen_orders.add(order_key)

            yield result


def _validate_row(row):
    """
    Validates a single CSV row.

    Returns:
        dict: The parsed row with:
            - normalized fields
            - is_valid flag
            - error_message string
    """
    errors = []

    # Validate required string fields
    customer_id = _parse_string_field(row.get('customer_id'))
    if customer_id is None:
        errors.append("Invalid or missing customer_id")

    order_id = _parse_string_field(row.get('order_id'))
    if order_id is None:
        errors.append("Invalid or missing order_id")

    item = _parse_string_field(row.get('item'))
    if item is None:
        errors.append("Invalid or missing item")

    # Validate numeric fields
    quantity = _parse_int_field(row.get('quantity'))
    if quantity is None:
        errors.append("Invalid or missing quantity")

    unit_price = _parse_float_field(row.get('unit_price'))
    if unit_price is None:
        errors.append("Invalid or missing unit_price")

    # Validate date
    parsed_date = _parse_date(row.get('date'))
    if parsed_date is None:
        errors.append("Invalid or missing date")

    return {
        'customer_id': customer_id,
        'order_id': order_id,
        'item': item,
        'quantity': quantity,
        'unit_price': unit_price,
        'date': parsed_date,
        'is_valid': len(errors) == 0,
        'error_message': "; ".join(errors) if errors else None
    }


def _parse_string_field(value):
    if value is None or value.strip() == "":
        return None
    return value.strip()


def _parse_int_field(value):
    if value is None or value.strip() == "":
        return None
    try:
        return int(value.strip())
    except (ValueError, TypeError):
        return None


def _parse_float_field(value):
    if value is None or value.strip() == "":
        return None
    try:
        return float(value.strip())
    except (ValueError, TypeError):
        return None


def _parse_date(value):
    """
    Parses a date string using multiple possible formats.

    Tries:
    - "%Y-%m-%d" (e.g., 2025-06-01)
    - "%d/%m/%Y" (e.g., 01/06/2025)
    - "%Y/%m/%d" (e.g., 2025/06/01)
    - "%B %d %Y" (e.g., June 4 2025)

    Returns:
        datetime.date or None
    """
    if not value:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%B %d %Y"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except (ValueError, TypeError):
            continue
    return None
