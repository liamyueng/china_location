#!/usr/bin/env python3
"""
车辆轨迹查询测试和性能测试
"""

import time
import random
from datetime import datetime, timedelta
from vehicle_tracker import VehicleTracker, find_in_circle


def test_circle_query():
    """测试圆形范围查询"""
    print("=" * 60)
    print("圆形范围查询测试")
    print("=" * 60)
    
    tracker = VehicleTracker()
    
    # 获取数据统计
    stats = tracker.get_stats()
    print(f"\n数据库统计:")
    print(f"  车辆数: {stats['vehicle_count']:,}")
    print(f"  轨迹数: {stats['track_count']:,}")
    
    if stats['track_count'] == 0:
        print("\n数据库为空，请先运行 generate_vehicle_data.py 生成数据")
        tracker.close()
        return
    
    # 测试查询点（中国主要城市）
    test_points = [
        ("北京天安门", 116.407, 39.904),
        ("上海外滩", 121.490, 31.240),
        ("广州塔", 113.324, 23.106),
        ("深圳市民中心", 114.057, 22.543),
        ("成都天府广场", 104.066, 30.572),
    ]
    
    # 测试不同半径
    radii = [500, 1000, 5000, 10000]  # 米
    
    print("\n" + "-" * 60)
    print("查询测试:")
    print("-" * 60)
    
    for name, lng, lat in test_points:
        print(f"\n📍 {name} ({lng}, {lat})")
        
        for radius in radii:
            start = time.time()
            
            # 先统计数量
            count = tracker.count_in_circle(lng, lat, radius)
            
            # 执行查询
            results = tracker.find_in_circle(lng, lat, radius, limit=100)
            
            elapsed = (time.time() - start) * 1000
            
            print(f"   半径 {radius:>5}m: 约 {count:>6,} 条记录, "
                  f"返回 {len(results):>3} 条, 耗时 {elapsed:>6.1f}ms")
            
            # 显示最近的几条记录
            if results and radius == 1000:
                print(f"      最近记录:")
                for r in results[:3]:
                    print(f"        - 车辆 {r['vehicle_id']}: {r['distance_m']:.0f}m, "
                          f"时间 {r['recorded_at']}")
    
    tracker.close()


def performance_test():
    """性能测试"""
    print("\n" + "=" * 60)
    print("性能测试")
    print("=" * 60)
    
    tracker = VehicleTracker()
    
    stats = tracker.get_stats()
    if stats['track_count'] == 0:
        print("数据库为空")
        tracker.close()
        return
    
    # 获取地理范围
    geo_range = stats['geo_range']
    if not geo_range:
        print("无法获取地理范围")
        tracker.close()
        return
    
    lng_min, lng_max = geo_range['lng']
    lat_min, lat_max = geo_range['lat']
    
    # 随机生成查询点
    num_queries = 100
    radius = 2000  # 2公里
    
    print(f"\n执行 {num_queries} 次随机查询 (半径 {radius}m)")
    print("-" * 60)
    
    total_time = 0
    total_results = 0
    
    for i in range(num_queries):
        # 随机位置
        lng = random.uniform(lng_min, lng_max)
        lat = random.uniform(lat_min, lat_max)
        
        start = time.time()
        results = tracker.find_in_circle(lng, lat, radius, limit=100)
        elapsed = time.time() - start
        
        total_time += elapsed
        total_results += len(results)
        
        if (i + 1) % 20 == 0:
            print(f"  完成 {i+1}/{num_queries} 次查询...")
    
    avg_time = total_time / num_queries * 1000
    avg_results = total_results / num_queries
    
    print(f"\n性能统计:")
    print(f"  总查询次数: {num_queries}")
    print(f"  平均查询时间: {avg_time:.1f} ms")
    print(f"  平均返回结果数: {avg_results:.1f}")
    print(f"  QPS (理论): {1000/avg_time:.1f}")
    
    tracker.close()


def test_vehicle_track():
    """测试查询特定车辆轨迹"""
    print("\n" + "=" * 60)
    print("车辆轨迹查询测试")
    print("=" * 60)
    
    tracker = VehicleTracker()
    
    # 查询第一辆车的轨迹
    vehicle_id = "V0000"
    
    print(f"\n查询车辆 {vehicle_id} 的轨迹:")
    
    start = time.time()
    tracks = tracker.get_vehicle_track(vehicle_id, limit=10)
    elapsed = (time.time() - start) * 1000
    
    print(f"  查询耗时: {elapsed:.1f}ms")
    print(f"  返回记录数: {len(tracks)}")
    
    if tracks:
        print(f"\n  轨迹示例:")
        for t in tracks[:5]:
            print(f"    {t['recorded_at']}: ({t['lng']:.4f}, {t['lat']:.4f}) "
                  f"速度 {t['speed']:.1f}km/h")
    
    tracker.close()


def demo_usage():
    """演示使用方法"""
    print("\n" + "=" * 60)
    print("使用示例")
    print("=" * 60)
    
    print("""
# 方式1: 使用便捷函数
from vehicle_tracker import find_in_circle

# 查找北京天安门1公里范围内的轨迹
results = find_in_circle(116.407, 39.904, 1000)
for r in results[:5]:
    print(f"车辆 {r['vehicle_id']}: 距离 {r['distance_m']:.0f}m")

# 方式2: 使用类（更多功能）
from vehicle_tracker import VehicleTracker
from datetime import datetime

with VehicleTracker() as tracker:
    # 圆形范围查询
    results = tracker.find_in_circle(
        lng=116.407,
        lat=39.904,
        radius_m=1000,
        start_time=datetime(2025, 1, 1),
        end_time=datetime(2025, 1, 31),
        limit=100
    )
    
    # 统计范围内记录数
    count = tracker.count_in_circle(116.407, 39.904, 1000)
    
    # 获取特定车辆轨迹
    tracks = tracker.get_vehicle_track("V0001", limit=100)
""")


if __name__ == '__main__':
    # 运行所有测试
    test_circle_query()
    performance_test()
    test_vehicle_track()
    demo_usage()
