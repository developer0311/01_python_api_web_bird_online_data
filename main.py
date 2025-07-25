from flask import Flask, request, jsonify
import psycopg2
import pandas as pd
from collections import OrderedDict
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

# Connection parameters
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_PASS = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")


def fetch_data(service_id):
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT,
            connect_timeout=10,  # Timeout in seconds
        )

        query = "SELECT * FROM service_details WHERE service_id = %s;"
        df = pd.read_sql_query(query, connection, params=(service_id,))

        return df

    except Exception as e:
        return None

    finally:
        if "connection" in locals():
            connection.close()


@app.route("/api/service_data", methods=["GET"])
def get_service_data():
    service_id = request.args.get("service_id")
    if not service_id or not service_id.isdigit():
        return jsonify({"error": "Invalid service_id parameter"}), 400
    service_id = int(service_id)

    if not service_id:
        return jsonify({"error": "Missing service_id parameter"}), 400

    data = fetch_data(service_id)
    if data is None or data.empty:
        return jsonify({"error": "No data found"}), 404

    grouped_data = OrderedDict()
    for _, row in data.iterrows():
        cat = row["category"]
        if cat not in grouped_data:
            grouped_data[cat] = []
        grouped_data[cat].append(row.to_dict())

    option_price_map = {row["option"]: row["price"] for _, row in data.iterrows()}
    keyword_price_map = {row["keyword"]: row["price"] for _, row in data.iterrows()}
    option_keyword_map = {row["option"]: row["keyword"] for _, row in data.iterrows()}

    return jsonify(
        {
            "grouped_data": grouped_data,
            "option_price_map": option_price_map,
            "keyword_price_map": keyword_price_map,
            "option_keyword_map": option_keyword_map,
        }
    )


port = int(os.environ.get("PORT", 5000))

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)
