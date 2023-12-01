from flask import Flask, jsonify
import pandas as pd
import os

app = Flask(__name__)

@app.route("/api/transactions", methods=["GET"])
def get_transactions():
    data_file_path = '../DATA/transactions.json'

    if os.path.exists(data_file_path):
        df = pd.read_json(data_file_path)
        return jsonify(df.to_dict(orient='records'))
    else:
        return jsonify({"error": "Data file not found"}), 404

@app.route("/api/customers", methods=["GET"])
def get_customers():
    data_file_path = '../DATA/customers.json'

    if os.path.exists(data_file_path):
        df = pd.read_json(data_file_path)
        return jsonify(df.to_dict(orient='records'))
    else:
        return jsonify({"error": "Data file not found"}), 404
    
@app.route("/api/external_data", methods=["GET"])
def get_external_data():
    data_file_path = '../DATA/external_data.json'

    if os.path.exists(data_file_path):
        df = pd.read_json(data_file_path)
        return jsonify(df.to_dict(orient='records'))
    else:
        return jsonify({"error": "Data file not found"}), 404

if __name__ == "__main__":
    app.run(debug=True)
