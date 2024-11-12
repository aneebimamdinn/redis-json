import os
import json
import redis
from flask import Flask, request, jsonify
from redis.commands.json.path import Path

app = Flask(__name__)

# Define the folder where user JSON files are stored
USER_DATA_FOLDER = 'user_data'

r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)


@app.route('/load_data/<int:user_id>', methods=['POST'])
def load_data(user_id):
    # Construct the filename for facets file based on user_id
    user_filename = f'user{user_id}Facets.json'
    file_path = os.path.join(USER_DATA_FOLDER, user_filename)

    # Check if the file exists
    if not os.path.exists(file_path):
        return jsonify({"error": f"File {user_filename} not found."}), 404

    # Load data from the JSON file
    with open(file_path, 'r') as file:
        user_data = json.load(file)

    # Redis key for this user's products
    redis_key = f"user:{user_id}:products"

    # Check if the data already exists in Redis
    if r.exists(redis_key):
        return jsonify({"error": "Data for this user already exists in Redis."}), 400

    # Use JSON.SET to store the data in Redis as JSON
    try:
        r.json().set(redis_key, '$', user_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"message": f"Data successfully loaded for user {user_id}."}), 200


# Helper function to generate facet labels based on price, category, and brand
def generate_facet_labels(product):
    labels = []

    # Facet for category
    labels.append(f"categ_{product['Category'].replace(' ', '_')}")

    # Facet for brand
    labels.append(f"brand_{product['Brand'].replace(' ', '_')}")

    # Facet for price range (rounded to nearest multiple of 10)
    price = product['Price']
    price_range = f"price_{(price // 10) * 10}_{(price // 10 + 1) * 10}"
    labels.append(price_range)

    return labels


# API endpoint to receive user_id and check/create the facet file
@app.route('/add_facet_labels', methods=['POST'])
def add_facet_labels():
    user_id = request.json.get('user_id')  # Get the user_id from the request JSON

    if not user_id:
        return jsonify({"error": "user_id is required"}), 400

    # Generate file paths
    user_file = os.path.join(USER_DATA_FOLDER, f"user{user_id}.json")
    facet_file = os.path.join(USER_DATA_FOLDER, f"user{user_id}Facets.json")

    # Check if the user's data file exists
    if not os.path.exists(user_file):
        return jsonify({"error": f"user{user_id}.json not found"}), 404

    # If facet file already exists, do not create a new one
    if os.path.exists(facet_file):
        return jsonify({"message": f"Facet file for user{user_id} already exists"}), 200

    # Read the user data file
    with open(user_file, 'r') as f:
        user_data = json.load(f)

    # Add facet labels to each product
    for product in user_data:
        product['facet_labels'] = generate_facet_labels(product)

    # Create the facet file with updated data
    with open(facet_file, 'w') as f:
        json.dump(user_data, f, indent=4)

    return jsonify({"message": f"Facet file created for user{user_id}"}), 201


# Endpoint to filter products based on facet labels
@app.route('/filter_data/<int:user_id>', methods=['GET'])
def filter_data(user_id):
    # Get the facet filters from query parameters
    facet_filters = request.args.getlist('facet_filter')
    if not facet_filters:
        return jsonify({"error": "At least one facet_filter parameter is required."}), 400

    redis_key = f"user:{user_id}:products"

    # Check if the data exists in Redis
    if not r.exists(redis_key):
        return jsonify({"error": "User data not found in Redis. Ensure data is loaded first."}), 404

    # Build a RedisJSON query for multiple facet filters
    query = "$[?("

    # Combine all facet filters with logical AND
    query_conditions = []
    for filter in facet_filters:
        # Each filter will be checked for exact match inside facet_labels array
        query_conditions.append(f"@.facet_labels[?(@ == '{filter}')]")

    # Join conditions with "&&" for AND logic
    query += " && ".join(query_conditions)
    query += ")]"

    print("Generated Query: ", query)

    try:
        # Use JSON.GET to retrieve the filtered data
        filtered_data = r.json().get(redis_key, query)

        # If no data is found matching the query
        if not filtered_data:
            return jsonify({"message": "No matching products found for the given facet filters."}), 404

        return jsonify(filtered_data), 200
    except Exception as e:
        return jsonify({"error": f"Redis query failed: {str(e)}"}), 500


if __name__ == '__main__':
    # Create user_data folder if it doesn't exist
    os.makedirs(USER_DATA_FOLDER, exist_ok=True)

    app.run(debug=True)
