#!/usr/bin/env python3
"""
将 ok_geo.csv 数据导入 PostgreSQL 数据库（不使用 PostGIS）
使用边界框 (bbox) 进行快速筛选，多边形数据存储为 JSON
"""

import csv
import sys
import json
import psycopg2
from psycopg2.extras import execute_batch

# 处理大字段
csv.field_size_limit(sys.maxsize)

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'china_geo_simple',
    'user': 'postgres',
    'password': 'postgres'  # 请根据实际情况修改
}

CSV_FILE = 'ok_geo.csv'


def create_database(config):
    """创建数据库"""
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


def create_tables(conn):
    """创建表结构"""
    cur = conn.cursor()
    
    # 删除旧表
    cur.execute("DROP TABLE IF EXISTS regions CASCADE")
    
    # 创建区域表（不使用 PostGIS）
    cur.execute("""
        CREATE TABLE regions (
            id BIGINT PRIMARY KEY,
            pid BIGINT,
            deep INTEGER,
            name VARCHAR(100),
            ext_path VARCHAR(500),
            center_lng DOUBLE PRECISION,
            center_lat DOUBLE PRECISION,
            -- 边界框，用于快速筛选
            bbox_min_lng DOUBLE PRECISION,
            bbox_max_lng DOUBLE PRECISION,
            bbox_min_lat DOUBLE PRECISION,
            bbox_max_lat DOUBLE PRECISION,
            -- 多边形点数据，存储为 JSON 数组
            polygon_json TEXT
        )
    """)
    
    # 创建索引
    cur.execute("CREATE INDEX idx_regions_deep ON regions(deep)")
    cur.execute("CREATE INDEX idx_regions_pid ON regions(pid)")
    cur.execute("CREATE INDEX idx_regions_name ON regions(name)")
    # 边界框索引，用于快速空间查询
    cur.execute("CREATE INDEX idx_regions_bbox ON regions(bbox_min_lng, bbox_max_lng, bbox_min_lat, bbox_max_lat)")
    
    conn.commit()
    cur.close()
    print("表结构创建完成")


def parse_polygon(polygon_str):
    """
    解析 polygon 字符串为点列表，同时计算边界框
    输入格式: "lng lat,lng lat,..." 或 "lng lat,lng lat,...;lng lat,lng lat,..." (多个多边形)
    对于多个多边形，只取第一个（主多边形）
    返回: (points_list, bbox) 或 (None, None)
    """
    if not polygon_str or polygon_str.strip() == '':
        return None, None
    
    try:
        # 如果有多个多边形（用分号分隔），取第一个（最大的主多边形）
        if ';' in polygon_str:
            polygon_parts = polygon_str.split(';')
            # 取第一个多边形（通常是主体）
            polygon_str = polygon_parts[0]
        
        points = []
        min_lng = float('inf')
        max_lng = float('-inf')
        min_lat = float('inf')
        max_lat = float('-inf')
        
        coords = polygon_str.split(',')
        
        for coord in coords:
            coord = coord.strip()
            if not coord:
                continue
            
            parts = coord.split()
            if len(parts) >= 2:
                lng = float(parts[0])
                lat = float(parts[1])
                points.append([lng, lat])
                
                # 更新边界框
                min_lng = min(min_lng, lng)
                max_lng = max(max_lng, lng)
                min_lat = min(min_lat, lat)
                max_lat = max(max_lat, lat)
        
        if len(points) < 3:
            return None, None
        
        bbox = (min_lng, max_lng, min_lat, max_lat)
        return points, bbox
        
    except (ValueError, IndexError):
        return None, None


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
        INSERT INTO regions (
            id, pid, deep, name, ext_path, 
            center_lng, center_lat,
            bbox_min_lng, bbox_max_lng, bbox_min_lat, bbox_max_lat,
            polygon_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            points, bbox = parse_polygon(row.get('polygon', ''))
            
            if points and bbox:
                polygon_json = json.dumps(points)
                batch_data.append((
                    region_id, pid, deep, name, ext_path,
                    center_lng, center_lat,
                    bbox[0], bbox[1], bbox[2], bbox[3],
                    polygon_json
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
               bbox_min_lng, bbox_max_lng, bbox_min_lat, bbox_max_lat
        FROM regions 
        WHERE name = '浦东新区'
    """)
    row = cur.fetchone()
    if row:
        print(f"  ID: {row[0]}")
        print(f"  名称: {row[1]}")
        print(f"  路径: {row[2]}")
        print(f"  中心: ({row[3]}, {row[4]})")
        print(f"  边界框: 经度[{row[5]:.4f}, {row[6]:.4f}] 纬度[{row[7]:.4f}, {row[8]:.4f}]")
    
    cur.close()


def main():
    print("=== 中国行政区划数据导入 PostgreSQL (无 PostGIS) ===\n")
    
    # 创建数据库
    try:
        create_database(DB_CONFIG)
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
