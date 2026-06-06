import random
from datetime import datetime, timedelta

FIRST_NAMES = [
    "James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
    "William","Barbara","David","Elizabeth","Richard","Susan","Joseph","Jessica",
    "Thomas","Sarah","Charles","Karen","Christopher","Lisa","Daniel","Nancy",
    "Matthew","Betty","Anthony","Margaret","Mark","Sandra","Donald","Ashley",
    "Steven","Dorothy","Paul","Kimberly","Andrew","Emily","Kenneth","Donna",
    "Joshua","Michelle","Kevin","Carol","Brian","Amanda","George","Melissa",
    "Timothy","Deborah"
]

LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
    "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson",
    "White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker",
    "Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores",
    "Green","Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell",
    "Carter","Roberts"
]

CITIES = [
    "New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia",
    "San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville",
    "Fort Worth","Columbus","Charlotte","Indianapolis","San Francisco","Seattle",
    "Denver","Nashville","Oklahoma City","El Paso","Washington","Boston",
    "Memphis","Louisville","Portland","Las Vegas","Milwaukee","Albuquerque"
]

STATES = [
    "NY","CA","IL","TX","AZ","PA","TX","CA","TX","CA","TX","FL",
    "TX","OH","NC","IN","CA","WA","CO","TN","OK","TX","DC","MA",
    "TN","KY","OR","NV","WI","NM"
]

CATEGORIES = [
    "electronics","jewelery","men's clothing","women's clothing",
    "home & kitchen","sports","books","beauty"
]

PRODUCT_ADJECTIVES = [
    "Premium","Ultra","Classic","Pro","Elite","Smart","Deluxe","Advanced",
    "Essential","Original","Portable","Wireless","Digital","Modern","Compact"
]

PRODUCT_NOUNS = [
    "Headphones","Watch","Jacket","Dress","Blender","Yoga Mat","Novel",
    "Serum","Laptop","Ring","Sneakers","Handbag","Coffee Maker","Dumbbells",
    "Cookbook","Foundation","Tablet","Necklace","Coat","Skirt","Toaster",
    "Resistance Band","Thriller","Moisturizer","Monitor","Bracelet","Boots",
    "Backpack","Air Fryer","Kettlebell","Biography","Lipstick","Keyboard",
    "Earrings","Hoodie","Scarf","Microwave","Jump Rope","Manga","Sunscreen"
]

ORDER_STATUSES = ["pending","confirmed","shipped","delivered","cancelled"]
PAYMENT_METHODS = ["credit_card","debit_card","upi","net_banking","wallet"]


def generate_customers(n=5000):
    rng = random.Random(42)
    customers = []
    for i in range(1, n + 1):
        first = FIRST_NAMES[(i - 1) % len(FIRST_NAMES)]
        last = LAST_NAMES[(i - 1) % len(LAST_NAMES)]
        city_idx = rng.randint(0, len(CITIES) - 1)
        created_days_ago = rng.randint(90, 730)
        customers.append({
            "customer_id": i,
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}{i}@example.com",
            "username": f"{first.lower()}{i}",
            "phone": f"555-{rng.randint(100,999)}-{rng.randint(1000,9999)}",
            "city": CITIES[city_idx],
            "state": STATES[city_idx],
            "zipcode": f"{rng.randint(10000,99999)}",
            "country": "US",
            "created_at": (
                datetime.utcnow() - timedelta(days=created_days_ago)
            ).isoformat(),
            "updated_at": (
                datetime.utcnow() - timedelta(days=rng.randint(1, 89))
            ).isoformat()
        })
    return customers


def mutate_customers(customers: list, n=50) -> list:
    rng = random.Random()
    now = datetime.utcnow().isoformat()
    to_change = rng.sample(customers, min(n, len(customers)))
    for c in to_change:
        field = rng.choice(['city', 'phone'])
        if field == 'city':
            idx = rng.randint(0, len(CITIES) - 1)
            c['city'] = CITIES[idx]
            c['state'] = STATES[idx]
        else:
            c['phone'] = f"555-{rng.randint(100,999)}-{rng.randint(1000,9999)}"
        c['updated_at'] = now
    return customers


def generate_products(n=500):
    rng = random.Random(42)
    products = []
    for i in range(1, n + 1):
        adj = PRODUCT_ADJECTIVES[(i - 1) % len(PRODUCT_ADJECTIVES)]
        noun = PRODUCT_NOUNS[(i - 1) % len(PRODUCT_NOUNS)]
        price = round(rng.uniform(5.0, 999.99), 2)
        category = CATEGORIES[(i - 1) % len(CATEGORIES)]
        products.append({
            "product_id": i,
            "title": f"{adj} {noun}",
            "category": category,
            "price": price,
            "cost_price": round(price * rng.uniform(0.4, 0.7), 2),
            "stock_quantity": rng.randint(0, 500),
            "rating": round(rng.uniform(1.0, 5.0), 1),
            "review_count": rng.randint(10, 500),
            "updated_at": (
                datetime.utcnow() - timedelta(days=rng.randint(1, 89))
            ).isoformat()
        })
    return products


def mutate_products(products: list, n=10) -> list:
    rng = random.Random()
    now = datetime.utcnow().isoformat()
    to_change = rng.sample(products, min(n, len(products)))
    for p in to_change:
        field = rng.choice(['price', 'stock_quantity'])
        if field == 'price':
            p['price'] = round(p['price'] * rng.uniform(0.85, 1.15), 2)
        else:
            p['stock_quantity'] = rng.randint(0, 500)
        p['updated_at'] = now
    return products


def generate_orders(
    n=500,
    start_order_id=1,
    start_item_id=1,
    date_from=None,
    date_to=None,
    num_customers=5000,
    num_products=500
):
    rng = random.Random()

    if date_from is None:
        date_from = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    if date_to is None:
        date_to = datetime.utcnow()

    total_seconds = max(1, int((date_to - date_from).total_seconds()))

    orders = []
    order_items = []
    item_id = start_item_id

    for order_idx in range(n):
        order_id = start_order_id + order_idx
        order_date = date_from + timedelta(seconds=rng.randint(0, total_seconds))
        customer_id = rng.randint(1, num_customers)
        status = rng.choice(ORDER_STATUSES)
        payment_method = rng.choice(PAYMENT_METHODS)
        num_items = rng.randint(1, 5)
        selected_products = rng.sample(range(1, num_products + 1), num_items)

        line_total_sum = 0.0
        for pid in selected_products:
            qty = rng.randint(1, 4)
            unit_price = round(rng.uniform(5.0, 999.99), 2)
            line_total = round(qty * unit_price, 2)
            line_total_sum += line_total
            order_items.append({
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": pid,
                "quantity": qty,
                "unit_price": unit_price,
                "line_total": line_total
            })
            item_id += 1

        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date.isoformat(),
            "status": status,
            "payment_method": payment_method,
            "shipping_city": rng.choice(CITIES),
            "total_amount": round(line_total_sum, 2),
            "updated_at": order_date.isoformat()
        })

    return orders, order_items


def generate_reviews(orders: list, start_review_id: int = 1, num_products: int = 500):
    rng = random.Random()
    reviews = []
    review_id = start_review_id
    delivered = [o for o in orders if o['status'] == 'delivered']

    REVIEW_TEXTS = [
        "Great product, very happy with it.",
        "Exactly as described. Fast shipping.",
        "Good quality for the price.",
        "Would definitely buy again.",
        "Decent product but packaging was damaged.",
        "Not what I expected but usable.",
        "Excellent build quality.",
        "Very satisfied with this purchase.",
        "Average product, nothing special.",
        "Highly recommend to everyone.",
        "Product stopped working after a week.",
        "Fantastic value for money.",
        "Shipping was slow but product is good.",
        "Better than expected quality.",
        "Will not buy again, disappointed."
    ]

    for order in delivered:
        if rng.random() < 0.60:
            reviewed_at = (
                datetime.fromisoformat(order['order_date'])
                + timedelta(days=rng.randint(1, 14))
            ).isoformat()
            reviews.append({
                "review_id": review_id,
                "order_id": order['order_id'],
                "product_id": rng.randint(1, num_products),
                "customer_id": order['customer_id'],
                "rating": round(rng.uniform(1.0, 5.0), 1),
                "review_text": rng.choice(REVIEW_TEXTS),
                "reviewed_at": reviewed_at,
                "updated_at": reviewed_at
            })
            review_id += 1

    return reviews