#!/usr/bin/env python3
"""
使用 SQL 直接查询坐标所在区域
演示如何在 PostgreSQL 中用原生 SQL 进行地理空间查询
"""

import psycopg2

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'china_geo',
    'user': 'postgres',
    'password': 'postgres'
}


def query_by_sql(lng: float, lat: float):
    """使用 SQL 查询坐标所在区域"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # 核心 SQL 查询
    sql = """
        SELECT 
            name,
            ext_path,
            deep,
            center_lng,
            center_lat,
            ST_Distance(
                ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
                ST_SetSRID(ST_Point(center_lng, center_lat), 4326)::geography
            ) AS distance_meters
        FROM regions
        WHERE ST_Contains(polygon, ST_SetSRID(ST_Point(%s, %s), 4326))
        ORDER BY deep DESC
    """
    
    cur.execute(sql, (lng, lat, lng, lat))
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return results


def main():
    print("=== SQL 直接查询示例 ===\n")
    
    test_coords = [
        (110.995, 22.918, "岑溪市中心"),
        (116.407, 39.904, "北京天安门"),
    ]
    
    for lng, lat, desc in test_coords:
        print(f"查询坐标: ({lng}, {lat}) - {desc}")
        print("-" * 60)
        
        results = query_by_sql(lng, lat)
        
        if not results:
            print("  未找到匹配区域")
        else:
            for name, path, deep, clng, clat, dist in results:
                level = ['省', '市', '区/县'][deep] if deep < 3 else f'L{deep}'
                print(f"  [{level}] {name}")
                print(f"       路径: {path}")
                print(f"       中心: ({clng}, {clat})")
                print(f"       距中心: {dist:.2f} 米")
        
        print()
    
    # 展示一些有用的 SQL 查询
    print("\n=== 常用 SQL 查询示例 ===\n")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # 1. 查询某个区域包含的所有下级区域
    print("1. 查询梧州市下所有区县:")
    cur.execute("""
        SELECT name, center_lng, center_lat 
        FROM regions 
        WHERE pid = (SELECT id FROM regions WHERE name = '梧州市' AND deep = 1)
        ORDER BY id
    """)
    for name, lng, lat in cur.fetchall():
        print(f"   - {name} ({lng}, {lat})")
    
    # 2. 查询距离某点最近的几个区县
    print("\n2. 距离 (110.995, 22.918) 最近的 5 个区县:")
    cur.execute("""
        SELECT name, ext_path,
               ST_Distance(
                   ST_SetSRID(ST_Point(110.995, 22.918), 4326)::geography,
                   ST_SetSRID(ST_Point(center_lng, center_lat), 4326)::geography
               ) AS dist
        FROM regions
        WHERE deep = 2
        ORDER BY dist
        LIMIT 5
    """)
    for name, path, dist in cur.fetchall():
        print(f"   - {name}: {dist/1000:.2f} km ({path})")
    
    # 3. 统计各省下级区县数量
    print("\n3. 各省直辖区县数量 (前 10):")
    cur.execute("""
        SELECT p.name, COUNT(d.id) as district_count
        FROM regions p
        JOIN regions d ON d.ext_path LIKE p.name || '%' AND d.deep = 2
        WHERE p.deep = 0
        GROUP BY p.name
        ORDER BY district_count DESC
        LIMIT 10
    """)
    for name, count in cur.fetchall():
        print(f"   - {name}: {count} 个")
    
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
