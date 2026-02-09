# -*- coding: utf-8 -*-
"""
岑溪市坐标验证脚本

岑溪市位置:
- 东经: 110°3′ ~ 111°22′
- 北纬: 22°37′ ~ 23°13′

转换为十进制:
- 经度: 110.05° ~ 111.367°
- 纬度: 22.617° ~ 23.217°
"""

from coordinate_query import CoordinateQuery, dms_to_decimal, query_location
from geo_data_loader import get_loader


def main():
    print("=" * 70)
    print("岑溪市坐标验证")
    print("=" * 70)
    
    # 1. 首先查看岑溪市在数据库中的边界信息
    loader = get_loader()
    loader.load()
    
    print("\n【1. 查找岑溪市数据】")
    cenxi = loader.get_region_by_name('岑溪')
    if cenxi:
        print(f"  名称: {cenxi.name}")
        print(f"  完整路径: {cenxi.ext_path}")
        print(f"  层级: {cenxi.deep} (0=省, 1=市, 2=区县)")
        print(f"  中心坐标: {cenxi.center}")
        if cenxi.bbox:
            print(f"  边界框: 经度 {cenxi.bbox[0]:.4f}~{cenxi.bbox[2]:.4f}, 纬度 {cenxi.bbox[1]:.4f}~{cenxi.bbox[3]:.4f}")
        print(f"  多边形数量: {len(cenxi.polygons)}")
    else:
        print("  未找到岑溪市数据!")
        return
    
    # 2. 转换岑溪市官方经纬度范围
    print("\n【2. 岑溪市官方经纬度范围】")
    # 东经110°3′~111°22′，北纬22°37′~23°13′
    lon_min = dms_to_decimal(110, 3)     # 110°3′
    lon_max = dms_to_decimal(111, 22)    # 111°22′
    lat_min = dms_to_decimal(22, 37)     # 22°37′
    lat_max = dms_to_decimal(23, 13)     # 23°13′
    
    print(f"  官方范围:")
    print(f"    经度: {lon_min:.4f}° ~ {lon_max:.4f}° (东经110°3′~111°22′)")
    print(f"    纬度: {lat_min:.4f}° ~ {lat_max:.4f}° (北纬22°37′~23°13′)")
    
    if cenxi.bbox:
        print(f"  数据库边界框:")
        print(f"    经度: {cenxi.bbox[0]:.4f}° ~ {cenxi.bbox[2]:.4f}°")
        print(f"    纬度: {cenxi.bbox[1]:.4f}° ~ {cenxi.bbox[3]:.4f}°")
    
    # 3. 测试岑溪市中心点
    print("\n【3. 坐标查询验证】")
    query = CoordinateQuery()
    
    # 使用数据库中的中心点
    if cenxi.center:
        center_lon, center_lat = cenxi.center
        print(f"\n  测试数据库中心坐标: ({center_lon}, {center_lat})")
        result = query.query(center_lon, center_lat)
        print(f"    查询结果: {result['full_path']}")
        print(f"    区县: {result['district']}")
    
    # 计算岑溪市范围的中心
    center_lon = (lon_min + lon_max) / 2
    center_lat = (lat_min + lat_max) / 2
    print(f"\n  测试官方范围中心: ({center_lon:.4f}, {center_lat:.4f})")
    result = query.query(center_lon, center_lat)
    print(f"    查询结果: {result['full_path']}")
    print(f"    区县: {result['district']}")
    
    # 4. 测试岑溪市范围内的多个点
    print("\n【4. 岑溪市范围内多点测试】")
    test_points = [
        (110.5, 22.9, "岑溪市西南"),
        (110.8, 22.85, "岑溪市中部偏南"),
        (111.0, 22.95, "岑溪市东部"),
        (110.6, 23.0, "岑溪市北部"),
        (110.9508, 22.9191, "岑溪市区附近（百度坐标）"),
    ]
    
    for lon, lat, desc in test_points:
        result = query.query(lon, lat)
        status = "✓" if result['district'] and '岑溪' in result['district'] else "✗"
        print(f"  {status} {desc}: ({lon}, {lat})")
        print(f"      → {result['full_path'] or '未找到'}")
    
    # 5. 测试边界外的点（应该不在岑溪市）
    print("\n【5. 边界外测试（不应该是岑溪市）】")
    outside_points = [
        (109.5, 22.9, "岑溪市以西"),
        (112.0, 23.0, "岑溪市以东"),
        (110.5, 24.0, "岑溪市以北"),
    ]
    
    for lon, lat, desc in outside_points:
        result = query.query(lon, lat)
        is_cenxi = result['district'] and '岑溪' in result['district']
        status = "✗" if is_cenxi else "✓"
        print(f"  {status} {desc}: ({lon}, {lat})")
        print(f"      → {result['full_path'] or '未找到'}")
    
    print("\n" + "=" * 70)
    print("验证完成")
    print("=" * 70)


if __name__ == '__main__':
    main()
