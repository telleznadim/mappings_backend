from flask import Flask, jsonify, request
import pyodbc
from flask_cors import CORS
from dotenv import dotenv_values
import pandas as pd
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# define your SQL Server connection parameters
config = dotenv_values(
    ".env")
server = config["sql_server"]
database = config["sql_database"]
uid = config["sql_uid"]
pwd = config["sql_pwd"]

connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};Encrypt=yes;TrustServerCertificate=no;'

print(connection_string)
# define your endpoint to handle the read request


# @app.route('/get_data/<int:data_id>', methods=['GET'])
# @app.route('/get_inventory_posting_group_table/', methods=['GET'])
# def get_inventory_posting_group_table():
#     try:
#         with pyodbc.connect(connection_string) as connection:

#             query = "SELECT * FROM dw_inventory_posting_group"

#             df = pd.read_sql_query(query, connection)

#             if not df.empty:
#                 # convert the DataFrame to a list of dictionaries
#                 data = df.to_dict(orient='records')
#                 return jsonify(data)
#             else:
#                 # return a 404 response if no data is found
#                 return jsonify({'status': 'not found'}), 404

#     except Exception as e:
#         return jsonify({'status': 'error', 'message': str(e)}), 500


# @app.route('/get_inventory_posting_group_table/', methods=['GET'])
# def get_inventory_posting_group_table():
#     try:
#         # get limit and offset from query parameters
#         limit = request.args.get('limit', default=100, type=int)
#         offset = request.args.get('offset', default=0, type=int)

#         with pyodbc.connect(connection_string) as connection:

#             # modify the SQL query to include limit and offset
#             query = f"SELECT * FROM dw_inventory_posting_group ORDER BY id OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

#             df = pd.read_sql_query(query, connection)

#             if not df.empty:
#                 # convert the DataFrame to a list of dictionaries
#                 data = df.to_dict(orient='records')
#                 return jsonify(data)
#             else:
#                 # return a 404 response if no data is found
#                 return jsonify({'status': 'not found'}), 404

#     except Exception as e:
#         return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/get_inventory_posting_group_table/', methods=['GET'])
def get_inventory_posting_group_table():
    try:
        # get limit and offset from query parameters
        limit = request.args.get('limit', default=100, type=int)
        offset = request.args.get('offset', default=0, type=int)
        order_by = request.args.get('order_by', default="id", type=str)
        order = request.args.get('order', default="asc", type=str)
        print(order_by)

        with pyodbc.connect(connection_string) as connection:

            # modify the SQL query to include count and limit/offset
            count_query = "SELECT COUNT(*) AS total_rows FROM dw_inventory_posting_group"
            df_count = pd.read_sql_query(count_query, connection)
            total_rows = df_count.to_dict(orient='records')[0]

            query = f"SELECT * FROM dw_inventory_posting_group ORDER BY {order_by} {order} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

            df = pd.read_sql_query(query, connection)

            if not df.empty:
                # convert the DataFrame to a list of dictionaries
                data = df.to_dict(orient='records')
                return jsonify({
                    'data': data,
                    'attributes': total_rows
                })
            else:
                # return a 404 response if no data is found
                return jsonify({'status': 'not found'}), 404

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/get_inventory_posting_group/<int:data_id>', methods=['GET'])
def get_data(data_id):
    try:
        with pyodbc.connect(connection_string) as connection:
            # execute the select query with the provided id and retrieve the results as a pandas DataFrame
            query = f"SELECT * FROM dw_inventory_posting_group WHERE Id = {data_id}"
            df = pd.read_sql(query, connection)

            if not df.empty:
                # retrieve the first row of the DataFrame and convert it to a dictionary
                row_dict = df.iloc[0].to_dict()
                return jsonify(row_dict)
            else:
                # return a 404 response if no data is found
                return jsonify({'status': 'not found'}), 404
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/insert_inventory_posting_group', methods=['POST'])
def insert_data():
    data = request.json  # assuming that the request data is in JSON format
    try:
        with pyodbc.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                # extract the column names and values from the request data
                columns = tuple(data.keys())
                values = tuple(data.values())
                # print(columns)
                # print(values)
                # print(len(data))

                # create the insert query dynamically based on the number of columns and values
                query = f"INSERT INTO dw_inventory_posting_group ({', '.join(columns)}) VALUES ({', '.join(['?']*len(values))})"

                # execute the insert query with the provided data
                cursor.execute(query, *values)
                connection.commit()  # commit the changes to the database
        return jsonify([{'status': 'success'}, data])
        # return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

# define your endpoint to handle the delete request


@app.route('/delete_inventory_posting_group/<int:data_id>', methods=['DELETE'])
def delete_data(data_id):
    try:
        with pyodbc.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                # execute the delete query with the provided id
                query = "DELETE FROM dw_inventory_posting_group WHERE id = ?"
                cursor.execute(query, data_id)
                connection.commit()  # commit the changes to the database
        return jsonify({'status': f'success, deleted id: {data_id}'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# define your endpoint to handle the update request
@app.route('/update_inventory_posting_group/<int:data_id>', methods=['PUT'])
def update_data(data_id):
    data = request.json  # assuming that the request data is in JSON format
    try:
        with pyodbc.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                # build the SET clause dynamically based on the keys of the incoming JSON data
                set_clause = ', '.join([f"{col} = ?" for col in data.keys()])
                set_params = list(data.values()) + [data_id]
                print(set_clause)
                print(set_params)

                # execute the update query with the dynamically generated SET clause and provided id
                query = f"UPDATE dw_inventory_posting_group SET {set_clause} WHERE Id = ?"
                cursor.execute(query, set_params)
                connection.commit()  # commit the changes to the database

        return jsonify({'status': f'success, updated Id: {data_id}'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    app.run()
    # app.run(host='0.0.0.0', port=5000)
