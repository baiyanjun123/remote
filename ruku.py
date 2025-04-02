import os
import psycopg
import pandas as pd
import time
from datetime import datetime, timedelta

# 数据库连接
def connect_to_db():
    conn = psycopg.connect(
        dbname="huodianshujuku",  # 数据库名称
        user="postgres",  # 用户名
        password="postgres",  # 密码
        host="localhost",  # 数据库服务器地址
        port="5432",  # 默认 PostgreSQL 端口
    )

    return conn


# 创建新表
# 创建新表
# 创建新表
def create_table_from_csv(csv_file):
    # 获取 CSV 文件名作为表名（可以自定义）
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    year = int(csv_file[14:18])
    month = int(csv_file[18:20])
    day = int(csv_file[20:22])
    hour = int(csv_file[23:25])
    min = int(csv_file[25:27])
    current_time = datetime(year, month, day, hour, min)
    new_time = current_time + timedelta(hours=8)
    time = f"{new_time}"
    name ="火警"+time[0:4] + "年" + time[5:7] + "月" + time[8:10] + "日" + time[11:13] + "时" + time[14:16] + "分"
    # 替换表名中的点（如果有）
    table_name = table_name.replace('.', '_')
    # 构建 SQL 创建表语句
    create_table_query = f"CREATE TABLE IF NOT EXISTS {name} (id SERIAL PRIMARY KEY"
    # 读取 CSV 文件内容
    df = pd.read_csv(csv_file, skiprows=1)

    # 获取 CSV 文件的列名
    columns = df.columns.tolist()

    # 对每一列名进行处理，移除或替换特殊字符
    clean_columns = []
    for col in columns:
        # 替换列名中的特殊字符，确保符合 SQL 语法
        clean_col = col.replace('#', '_').replace('^', '_').replace(' ', '_').replace('.', '_').replace('(', '_').replace(')', '_').replace('-', '_')  # 更多替换规则
        clean_columns.append(f"\"{clean_col}\"")
        create_table_query += f", \"{clean_col}\" TEXT"

    create_table_query += ");"

    # 执行创建表的 SQL
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Table {table_name} created successfully!")



# 上传 CSV 文件数据到数据库
# 上传 CSV 文件数据到数据库
def upload_csv_to_db(csv_file):
    # 获取 CSV 文件名作为表名（可以自定义）
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    year = int(csv_file[14:18])
    month = int(csv_file[18:20])
    day = int(csv_file[20:22])
    hour = int(csv_file[23:25])
    min = int(csv_file[25:27])
    current_time = datetime(year, month, day, hour, min)
    new_time = current_time + timedelta(hours=8)
    time = f"{new_time}"
    name ="火警"+time[0:4] + "年" + time[5:7] + "月" + time[8:10] + "日" + time[11:13] + "时" + time[14:16] + "分"

    table_name = table_name.replace('.', '_')

    # 使用 pandas 读取 CSV 文件
    df = pd.read_csv(csv_file, skiprows=1)

    # 连接到 PostgreSQL 数据库
    conn = connect_to_db()
    cur = conn.cursor()

    # 获取 CSV 文件的列名
    columns = df.columns.tolist()

    # 对每一列名进行处理，移除或替换特殊字符
    clean_columns = []
    for col in columns:
        # 替换列名中的特殊字符，确保符合 SQL 语法
        clean_col = col.replace('#', '_').replace('^', '_').replace(' ', '_').replace('.', '_').replace('(', '_').replace(')', '_').replace('-', '_')  # 更多替换规则
        clean_columns.append(f"\"{clean_col}\"")

    # 确保插入列名正确
    insert_query = f"INSERT INTO {name} ({', '.join(clean_columns)}) VALUES ({', '.join(['%s'] * len(clean_columns))})"

    # 插入每行数据
    for _, row in df.iterrows():
        cur.execute(insert_query, tuple(row))

    # 提交事务并关闭连接
    conn.commit()
    cur.close()
    conn.close()
    print(f"Data from {csv_file} has been uploaded to {name}.")




def get_uploaded_files():
    try:
        with open("C:\\kuihua\\uploaded_files.txt", "r") as f:
            return set(f.read().splitlines())
    except FileNotFoundError:
        return set()

def mark_file_as_uploaded(file_name):
    with open("C:\\kuihua\\uploaded_files.txt", "a") as f:
        f.write(file_name + "\n")

def monitor_folder(folder_path):
    uploaded_files = get_uploaded_files()

    while True:
        # 获取文件夹中的所有文件
        files = os.listdir(folder_path)
        csv_files = [f for f in files if f.endswith('.csv')]

        # 如果有未上传的 CSV 文件，处理这些文件
        for csv_file in csv_files:
            if csv_file not in uploaded_files:  # 如果文件没有上传过，才处理
                full_path = os.path.join(folder_path, csv_file)

                # 创建表
                create_table_from_csv(full_path)

                # 上传 CSV 数据
                upload_csv_to_db(full_path)

                # 上传完成后，记录该文件已上传
                mark_file_as_uploaded(csv_file)
                uploaded_files.add(csv_file)

        # 每隔一定时间检测一次文件夹
        time.sleep(10)  # 每10秒检测一次


# 主函数
if __name__ == "__main__":
    # 设置监控文件夹路径
    folder_path = 'C:\\kuihua'  # 替换为你的文件夹路径
    monitor_folder(folder_path)
