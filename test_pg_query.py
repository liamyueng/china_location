#!/usr/bin/env python3
"""
测试 PostgreSQL 坐标查询
"""

from pg_query import PGLocationQuery, find_location

def main():
    print("=== PostgreSQL 坐标查询测试 ===\n")
    
    # 使用便捷函数
    print("1. 使用便捷函数 find_location():")
    print("-" * 50)
    result = find_location(121.544, 31.221)
    print(f"   find_location(121.544, 31.221)")
    print(f"   => {result}")
    
    # 使用类
    print("\n2. 使用 PGLocationQuery 类:")
    print("-" * 50)
    
    with PGLocationQuery() as query:
        # 多个测试点
        test_points = [
            (121.544, 31.221, "浦东新区中心"),
            (116.407, 39.904, "北京天安门"),
            (121.474, 31.230, "上海外滩"),
            (113.264, 23.129, "广州"),
            (114.057, 22.543, "深圳"),
            (104.066, 30.659, "成都"),
            (106.551, 29.563, "重庆"),
            (120.154, 30.287, "杭州"),
        ]
        
        for lng, lat, desc in test_points:
            result = query.find_location(lng, lat)
            province = result.get('province') or '-'
            city = result.get('city') or '-'
            district = result.get('district') or '-'
            print(f"   ({lng}, {lat}) [{desc}]")
            print(f"   => {province} / {city} / {district}")
            print()
        
        # 批量查询示例
        print("3. 批量查询:")
        print("-" * 50)
        coords = [(121.544, 31.221), (116.407, 39.904), (121.474, 31.230)]
        results = query.batch_find(coords)
        for r in results:
            coord = r['coordinate']
            print(f"   {coord} => {r.get('city')} {r.get('district')}")
    
    print("\n查询完成！")


if __name__ == '__main__':
    main()
