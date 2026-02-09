#!/usr/bin/env python3
"""
测试无 PostGIS 的 PostgreSQL 坐标查询
"""

from pg_simple_query import PGSimpleQuery, find_location

def main():
    print("=== PostgreSQL 坐标查询测试 (无 PostGIS) ===\n")
    
    # 使用便捷函数
    print("1. 使用便捷函数 find_location():")
    print("-" * 50)
    result = find_location(110.995, 22.918)
    print(f"   find_location(110.995, 22.918)")
    print(f"   => {result}")
    
    # 使用类
    print("\n2. 多个坐标测试:")
    print("-" * 50)
    
    with PGSimpleQuery() as query:
        test_points = [
            (110.995, 22.918, "岑溪市中心"),
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
            print(f"   ({lng:>7}, {lat:>6}) [{desc:^8}] => {province} / {city} / {district}")
        
        # 详细查询
        print("\n3. 详细查询 (110.995, 22.918):")
        print("-" * 50)
        for item in query.find_location_detail(110.995, 22.918):
            level_name = ['省', '市', '区县'][item['level']] if item['level'] < 3 else f"L{item['level']}"
            print(f"   [{level_name}] {item['name']}, 距中心约 {item['distance_approx_m']:.0f}米")
        
        # 批量查询
        print("\n4. 批量查询:")
        print("-" * 50)
        coords = [(110.995, 22.918), (116.407, 39.904), (121.474, 31.230)]
        results = query.batch_find(coords)
        for r in results:
            coord = r['coordinate']
            print(f"   {coord} => {r.get('city') or '-'} {r.get('district') or '-'}")
        
        # 查找下级区划
        print("\n5. 查询梧州市下级区划:")
        print("-" * 50)
        # 先找梧州市 ID
        wuzhou = query.find_by_name('梧州市')
        if wuzhou:
            wuzhou_id = wuzhou[0]['id']
            for child in query.get_children(wuzhou_id):
                print(f"   - {child['name']} ({child['center'][0]:.4f}, {child['center'][1]:.4f})")
    
    print("\n查询完成！")


if __name__ == '__main__':
    main()
