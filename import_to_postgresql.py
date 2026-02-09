#!/usr/bin/env python3
"""
将 ok_geo.csv 数据导入 PostgreSQL 数据库（使用 PostGIS 扩展）
"""

import csv
import sys
import psycopg2
from psycopg2.extras import execute_batch

# 处理大字段
csv.field_size_limit(sys.maxsize)

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'china_geo',
    'user': 'postgres',
    'password': 'postgres'  # 请根据实际情况修改
}

CSV_FILE = 'ok_geo.csv'


def create_database_and_extension(config):
    """创建数据库和 PostGIS 扩展"""
    # 先连接到默认数据库创建目标数据库
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        database='postgres',
        user=config['user'],
        password=config['password']
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # 检查数据库是否存在
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (config['database'],))
    if not cur.fetchone():
        print(f"创建数据库 {config['database']}...")
        cur.execute(f"CREATE DATABASE {config['database']}")
    else:
        print(f"数据库 {config['database']} 已存在")
    
    cur.close()
    conn.close()
    
    # 连接到目标数据库，创建 PostGIS 扩展
    conn = psycopg2.connect(**config)
    conn.autocommit = True
    cur = conn.cursor()
    
    print("启用 PostGIS 扩展...")
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    
    cur.close()
    conn.close()


def create_tables(conn):
    """创建表结构"""
    cur = conn.cursor()
    
    # 删除旧表
    cur.execute("DROP TABLE IF EXISTS regions CASCADE")
    
    # 创建区域表
    cur.execute("""
        CREATE TABLE regions (
            id BIGINT PRIMARY KEY,
            pid BIGINT,
            deep INTEGER,
            name VARCHAR(100),
            ext_path VARCHAR(500),
            center_lng DOUBLE PRECISION,
            center_lat DOUBLE PRECISION,
            polygon GEOMETRY(MULTIPOLYGON, 4326)
        )
    """)
    
    # 创建索引
    cur.execute("CREATE INDEX idx_regions_deep ON regions(deep)")
    cur.execute("CREATE INDEX idx_regions_pid ON regions(pid)")
    cur.execute("CREATE INDEX idx_regions_name ON regions(name)")
    cur.execute("CREATE INDEX idx_regions_polygon ON regions USING GIST(polygon)")
    
    conn.commit()
    cur.close()
    print("表结构创建完成")


def parse_polygon_to_wkt(polygon_str):
    """
    将 CSV 中的 polygon 字符串转换为 WKT 格式的 MULTIPOLYGON
    输入格式: "lng lat,lng lat,..." （空格分隔经纬度，逗号分隔点）
    输出格式: MULTIPOLYGON(((lng lat, lng lat, ...)))
    """
    if not polygon_str or polygon_str.strip() == '':
        return None
    
    try:
        points = []
        # 用逗号分隔各个点
        coords = polygon_str.split(',')
        
        for coord in coords:
            coord = coord.strip()
            if not coord:
                continue
            
            # 格式: "lng lat"
            parts = coord.split()
            if len(parts) >= 2:
                lng = float(parts[0])
                lat = float(parts[1])
                points.append(f"{lng} {lat}")
        
        if len(points) < 3:
            return None
        
        # 确保多边形闭合
        if points[0] != points[-1]:
            points.append(points[0])
        
        return f"MULTIPOLYGON((({', '.join(points)})))"
        
    except (ValueError, IndexError) as e:
        return None


def parse_center(geo_str):
    """解析中心点坐标，格式: "lng lat" """
    if not geo_str or geo_str.strip() == '':
        return None, None
    
    try:
        parts = geo_str.split()
        if len(parts) >= 2:
            return float(parts[0]), float(parts[1])
    except (ValueError, IndexError):
        pass
    
    return None, None


def import_data(conn, csv_file):
    """导入 CSV 数据"""
    cur = conn.cursor()
    
    # 读取 CSV (使用 utf-8-sig 处理 BOM)
    print(f"读取 {csv_file}...")
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"共 {len(rows)} 条记录")
    
    # 插入 SQL
    insert_sql = """
        INSERT INTO regions (id, pid, deep, name, ext_path, center_lng, center_lat, polygon)
        VALUES (%s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
    """
    
    batch_data = []
    success_count = 0
    error_count = 0
    
    for i, row in enumerate(rows):
        try:
            region_id = int(row['id'])
            pid = int(row['pid']) if row['pid'] else 0
            deep = int(row['deep']) if row['deep'] else 0
            name = row['name']
            ext_path = row.get('ext_path', '')
            
            center_lng, center_lat = parse_center(row.get('geo', ''))
            polygon_wkt = parse_polygon_to_wkt(row.get('polygon', ''))
            
            if polygon_wkt:
                batch_data.append((
                    region_id, pid, deep, name, ext_path,
                    center_lng, center_lat, polygon_wkt
                ))
                success_count += 1
            else:
                error_count += 1
                if error_count <= 5:
                    print(f"  无效 polygon [{i}]: {name}")
        except Exception as e:
            error_count += 1
            if error_count <= 5:
                print(f"  解析错误 [{i}]: {e}")
        
        if (i + 1) % 500 == 0:
            print(f"  已处理 {i + 1}/{len(rows)}...")
    
    print(f"插入数据库（{len(batch_data)} 条）...")
    execute_batch(cur, insert_sql, batch_data, page_size=100)
    
    conn.commit()
    cur.close()
    
    print(f"导入完成: 成功 {success_count}, 失败 {error_count}")


def verify_data(conn):
    """验证导入的数据"""
    cur = conn.cursor()
    
    print("\n=== 数据验证 ===")
    
    # 统计各级别数量
    cur.execute("SELECT deep, COUNT(*) FROM regions GROUP BY deep ORDER BY deep")
    for deep, count in cur.fetchall():
        level_name = ['省/直辖市', '地级市', '区/县'][deep] if deep < 3 else f'级别{deep}'
        print(f"  {level_name}: {count}")
    
    # 查询浦东新区
    print("\n查询浦东新区...")
    cur.execute("""
        SELECT id, name, ext_path, center_lng, center_lat,
               ST_AsText(ST_Centroid(polygon)) as centroid
        FROM regions 
        WHERE name = '浦东新区'
    """)
    row = cur.fetchone()
    if row:
        print(f"  ID: {row[0]}")
        print(f"  名称: {row[1]}")
        print(f"  路径: {row[2]}")
        print(f"  中心: ({row[3]}, {row[4]})")
        print(f"  质心: {row[5]}")
    
    # 测试坐标查询
    print("\n测试坐标查询 (121.544, 31.221)...")
    cur.execute("""
        SELECT name, ext_path, deep
        FROM regions
        WHERE ST_Contains(polygon, ST_SetSRID(ST_Point(121.544, 31.221), 4326))
        ORDER BY deep DESC
    """)
    results = cur.fetchall()
    for name, path, deep in results:
        level_name = ['省', '市', '区县'][deep] if deep < 3 else f'级别{deep}'
        print(f"  [{level_name}] {name} ({path})")
    
    cur.close()


def main():
    print("=== 中国行政区划数据导入 PostgreSQL ===\n")
    
    # 创建数据库和扩展
    try:
        create_database_and_extension(DB_CONFIG)
    except Exception as e:
        print(f"创建数据库失败: {e}")
        print("请确保 PostgreSQL 已安装并运行，且密码正确")
        return
    
    # 连接数据库
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # 创建表
        create_tables(conn)
        
        # 导入数据
        import_data(conn, CSV_FILE)
        
        # 验证
        verify_data(conn)
        
    finally:
        conn.close()
    
    print("\n导入完成！")


if __name__ == '__main__':
    main()
