#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sqlite3

from flask import Flask, request, jsonify
from uvicorn.middleware.wsgi import WSGIMiddleware

app = Flask(__name__)
DATABASE = './stock_data.db'  # Replace with your actual database file

def get_db_connection(db_name):
    db_path = f"./{db_name}.db"
    if not os.path.exists(db_path):
        db_path = "./stock_data.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/execute', methods=['POST'])
def execute_sql():
    sql_query = request.json.get('query')
    db_name = request.json.get('db_name')
    if not sql_query:
        return jsonify({'error': 'No SQL query provided'}), 400

    try:
        conn = get_db_connection(db_name)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        conn.commit()
        if sql_query.strip().lower().startswith('select'):
            rows = cursor.fetchall()
            result = [dict(row) for row in rows]
            return jsonify(result)
        else:
            return jsonify({'message': 'Query executed successfully'})
    except sqlite3.Error as e:
        return jsonify({'error': str(e)}), 400
    finally:
        conn.close()

# Wrap the Flask app with WSGIMiddleware
app = WSGIMiddleware(app)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
