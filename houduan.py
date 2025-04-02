from flask import Flask, jsonify
from flask_cors import CORS  # 允许跨域请求
import psycopg

app = Flask(__name__)
CORS(app)  # 启用 CORS，允许跨端口请求

# 数据库连接
def connect_to_db():
    conn = psycopg.connect(
        dbname="huodianshujuku",  # 替换为你的数据库名称
        user="postgres",  # 用户名
        password="postgres",  # 密码
        host="localhost",  # 数据库服务器地址
        port="5432",  # PostgreSQL 默认端口
    )
    return conn

# 获取数据库中所有的表名
@app.route('/api/tables', methods=['GET'])
def get_tables():
    conn = connect_to_db()
    cur = conn.cursor()

    # 查询所有表名
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    rows = cur.fetchall()

    tables = [row[0] for row in rows]

    cur.close()
    conn.close()

    return jsonify(tables)

# 获取特定表的点数据
@app.route('/api/points/<table_name>', methods=['GET'])
def get_points(table_name):
    conn = connect_to_db()
    cur = conn.cursor()

    # 查询指定表中的所有点数据（假设表有 'latitude', 'longitude' 和 'other_info' 列）
    cur.execute(f"SELECT \"Lat\", \"Lon\", \"Reliability\" FROM {table_name}")
    rows = cur.fetchall()

    # 将 latitude 和 longitude 转换为数字类型
    points = [{"latitude": float(row[0]), "longitude": float(row[1]), "Reliability": row[2]} for row in rows]

    cur.close()
    conn.close()

    return jsonify(points)

if __name__ == '__main__':
    app.run(debug=True, port=8081)  # 后端 Flask 监听 8081 端口
