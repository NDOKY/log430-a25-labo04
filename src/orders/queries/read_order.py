"""
Orders (read-only model)
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
import json
import time
from collections import defaultdict
import logging
from db import get_redis_conn, get_sqlalchemy_session
from collections import defaultdict
from logger import Logger
from orders.models.order import Order
from orders.models.order_item import OrderItem
from sqlalchemy.sql import func

logger = Logger.get_instance("order_reports")

def get_order_by_id(order_id):
    """Get order by ID from Redis"""
    r = get_redis_conn()
    raw_order = r.hgetall(f"order:{order_id}")
    order = {}
    for key, value in raw_order.items():
        found_key = key.decode('utf-8') if isinstance(key, bytes) else key
        found_value = value.decode('utf-8') if isinstance(value, bytes) else value
        order[found_key] = found_value
    return order

def get_highest_spending_users_mysql():
    """Get report of highest spending users from MySQL"""
    session = get_sqlalchemy_session()
    limit = 10
    
    try:
        results = session.query(
            Order.user_id,
            func.sum(Order.total_amount).label('total_expense')
        ).group_by(Order.user_id)\
         .order_by(func.sum(Order.total_amount).desc())\
         .limit(limit)\
         .all()
        
        return [
            {
                "user_id": result.user_id,
                "total_expense": round(float(result.total_expense), 2)
            }
            for result in results
        ]
    finally:
        session.close()

def get_best_selling_products_mysql():
    """Get report of best selling products by quantity sold from MySQL"""
    session = get_sqlalchemy_session()
    limit = 100
    result = []
    
    try:
        order_items = session.query(
            OrderItem.product_id,
            func.sum(OrderItem.quantity).label('total_sold')
        ).group_by(OrderItem.product_id)\
         .order_by(func.sum(OrderItem.quantity).desc())\
         .limit(limit)\
         .all()
        
        for order_item in order_items:
            result.append({
                "product_id": order_item[0],
                "quantity": round(order_item[1], 2)
            })

        return result

    finally:
        session.close()


logger = logging.getLogger(__name__)

def get_highest_spending_users_redis():
    """Get report of highest spending users from Redis.
    Returns:
        list[dict]: A list of {"user_id": int, "total_expense": float} or {"error": str}.
    """
    result = []
    start_time = time.time()

    try:
        r = get_redis_conn()

        # Vérifier le cache
        report_in_cache = r.get("reports:highest_spending_users")
        if report_in_cache:
            return json.loads(report_in_cache)

        limit = 10
        spending = defaultdict(float)

        # Parcours efficace des commandes
        for key in r.scan_iter("order:*"):
            order_data = r.hgetall(key)
            if "user_id" in order_data and "total_amount" in order_data:
                try:
                    user_id = int(order_data["user_id"])
                    total = float(order_data["total_amount"])
                    spending[user_id] += total
                except ValueError:
                    continue  # Ignore les valeurs corrompues

        # Trier par dépense décroissante et limiter
        highest_spending_users = sorted(spending.items(), key=lambda x: x[1], reverse=True)[:limit]
        result = [
            {"user_id": user_id, "total_expense": round(total, 2)}
            for user_id, total in highest_spending_users
        ]

        # Mettre en cache pour 60 secondes
        r.set("reports:highest_spending_users", json.dumps(result), ex=60)

    except Exception as e:
        return {"error": str(e)}
    else:
        end_time = time.time()
        logger.debug(f"Executed in {end_time - start_time:.4f} seconds")
        return result
  

def get_best_selling_products_redis():
    """Get report of best selling products by quantity sold from Redis."""
    result = []
    start_time = time.time()
    try:
        r = get_redis_conn()
        report_in_cache = r.get("reports:best_selling_products")
        if report_in_cache:
            return json.loads(report_in_cache)

        limit = 10
        product_sales = defaultdict(int)

        for order_key in r.scan_iter("order:*"):
            order_data = r.hgetall(order_key)
            if "items" in order_data:
                try:
                    products = json.loads(order_data["items"])
                except Exception:
                    continue

                for item in products:
                    product_id = int(item.get("product_id", 0))
                    quantity = int(item.get("quantity", 0))
                    product_sales[product_id] += quantity

        best_selling = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[:limit]
        result = [{"product_id": pid, "quantity_sold": qty} for pid, qty in best_selling]

        # Cache result for 60 seconds
        r.set("reports:best_selling_products", json.dumps(result), ex=60)

    except Exception as e:
        return {"error": str(e)}

    end_time = time.time()
    logger.debug(f"Executed in {end_time - start_time:.4f} seconds")
    return result


def get_highest_spending_users():
    """Get report of highest spending users"""
    return get_highest_spending_users_redis()

def get_best_selling_products():
    """Get report of best selling products"""
    return get_best_selling_products_redis()