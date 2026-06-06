import json
import logging
import os
from datetime import datetime, timedelta

from data_generator import (
    generate_customers, mutate_customers,
    generate_products, mutate_products,
    generate_orders, generate_reviews
)
from utils import upload_to_s3, load_from_s3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

MARKER_FILE = '.last_run'
NUM_CUSTOMERS = 5000
NUM_PRODUCTS = 500
BASE_ORDERS = 5000
DAILY_ORDERS = 500


def read_marker() -> dict:
    with open(MARKER_FILE, 'r') as f:
        return json.load(f)


def write_marker(data: dict):
    with open(MARKER_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def is_first_run() -> bool:
    return not os.path.exists(MARKER_FILE)


def already_ran_today(marker: dict) -> bool:
    last = marker.get('last_run', '')[:10]
    today = datetime.utcnow().strftime('%Y-%m-%d')
    return last == today


def _stamp(records: list, ts: str) -> list:
    for r in records:
        r['_extracted_at'] = ts
    return records


def run():
    now = datetime.utcnow()
    ts = now.isoformat()

    if is_first_run():
        logger.info("=== FIRST RUN: Full base load ===")

        customers = generate_customers(n=NUM_CUSTOMERS)
        upload_to_s3(_stamp(customers, ts), 'customers', now)
        logger.info(f"Customers: {len(customers)} records uploaded")

        products = generate_products(n=NUM_PRODUCTS)
        upload_to_s3(_stamp(products, ts), 'products', now)
        logger.info(f"Products: {len(products)} records uploaded")

        date_from = now - timedelta(days=90)
        orders, order_items = generate_orders(
            n=BASE_ORDERS,
            start_order_id=1,
            start_item_id=1,
            date_from=date_from,
            date_to=now,
            num_customers=NUM_CUSTOMERS,
            num_products=NUM_PRODUCTS
        )
        upload_to_s3(_stamp(orders, ts), 'orders', now)
        upload_to_s3(_stamp(order_items, ts), 'order_items', now)
        logger.info(f"Orders: {len(orders)} | Order items: {len(order_items)}")

        reviews = generate_reviews(orders, start_review_id=1)
        upload_to_s3(_stamp(reviews, ts), 'reviews', now)
        logger.info(f"Reviews: {len(reviews)} records uploaded")

        write_marker({
            'last_run': now.isoformat(),
            'next_order_id': BASE_ORDERS + 1,
            'next_item_id': len(order_items) + 1,
            'next_review_id': len(reviews) + 1,
            'run_number': 1
        })
        logger.info("=== Base load complete ===")

    else:
        marker = read_marker()

        if already_ran_today(marker):
            logger.info("Already ran today — exiting. Safe to retry.")
            return

        run_number = marker['run_number'] + 1
        logger.info(f"=== INCREMENTAL RUN #{run_number} ===")

        # customers — full snapshot with ~50 mutations
        customers = generate_customers(n=NUM_CUSTOMERS)
        customers = mutate_customers(customers, n=50)
        upload_to_s3(_stamp(customers, ts), 'customers', now)
        logger.info(f"Customers: {len(customers)} uploaded (~50 mutated)")

        # products — full snapshot with ~10 mutations
        products = generate_products(n=NUM_PRODUCTS)
        products = mutate_products(products, n=10)
        upload_to_s3(_stamp(products, ts), 'products', now)
        logger.info(f"Products: {len(products)} uploaded (~10 mutated)")

        # new orders for today
        next_order_id = marker['next_order_id']
        next_item_id = marker['next_item_id']
        next_review_id = marker['next_review_id']

        orders, order_items = generate_orders(
            n=DAILY_ORDERS,
            start_order_id=next_order_id,
            start_item_id=next_item_id,
            date_from=now - timedelta(hours=24),
            date_to=now,
            num_customers=NUM_CUSTOMERS,
            num_products=NUM_PRODUCTS
        )
        upload_to_s3(_stamp(orders, ts), 'orders', now)
        upload_to_s3(_stamp(order_items, ts), 'order_items', now)
        logger.info(f"Orders: {len(orders)} | Items: {len(order_items)}")

        reviews = generate_reviews(orders, start_review_id=next_review_id)
        upload_to_s3(_stamp(reviews, ts), 'reviews', now)
        logger.info(f"Reviews: {len(reviews)} records uploaded")

        write_marker({
            'last_run': now.isoformat(),
            'next_order_id': next_order_id + DAILY_ORDERS,
            'next_item_id': next_item_id + len(order_items),
            'next_review_id': next_review_id + len(reviews),
            'run_number': run_number
        })
        logger.info(f"=== Incremental run #{run_number} complete ===")


if __name__ == "__main__":
    run()