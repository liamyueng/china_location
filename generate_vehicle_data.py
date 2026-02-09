#!/usr/bin/env python3
"""
生成模拟车辆轨迹数据
200辆车，1000万条轨迹记录
"""

import random
import math
from datetime import datetime, timedelta
from typing import List, Tuple, Generator
import time

from vehicle_tracker import VehicleTracker


# 中国主要城市中心坐标（作为车辆活动区域）
CITY_CENTERS = [
    # (城市名, 经度, 纬度, 活动半径km)
    ("北京", 116.407, 39.904, 50),
    ("上海", 121.474, 31.230, 40),
    ("广州", 113.264, 23.129, 35),
    ("深圳", 114.057, 22.543, 30),
    ("成都", 104.066, 30.572, 35),
    ("杭州", 120.153, 30.287, 30),
    ("武汉", 114.305, 30.593, 35),
    ("西安", 108.940, 34.341, 30),
    ("南京", 118.796, 32.060, 30),
    ("重庆", 106.551, 29.563, 40),
]


def generate_vehicle_id(index: int) -> str:
    """生成车辆ID"""
    return f"V{index:04d}"


def generate_plate_number(city_index: int) -> str:
    """生成车牌号"""
    provinces = ['京', '沪', '粤', '粤', '川', '浙', '鄂', '陕', '苏', '渝']
    letters = 'ABCDEFGHJKLMNPQRSTUVWXYZ'
    
    province = provinces[city_index % len(provinces)]
    letter = random.choice(letters)
    numbers = ''.join([str(random.randint(0, 9)) for _ in range(5)])
    
    return f"{province}{letter}{numbers}"


def simulate_vehicle_movement(
    start_lng: float,
    start_lat: float,
    city_radius_km: float,
    city_center_lng: float,
    city_center_lat: float,
    num_points: int,
    start_time: datetime,
    interval_seconds: int = 30
) -> Generator[Tuple[float, float, float, float, datetime], None, None]:
    """
    模拟车辆移动轨迹
    
    Yields:
        (lng, lat, speed, direction, timestamp)
    """
    lng, lat = start_lng, start_lat
    current_time = start_time
    
    # 车辆状态
    speed = random.uniform(20, 60)  # km/h
    direction = random.uniform(0, 360)  # 度
    
    for _ in range(num_points):
        # 添加一些随机性
        speed += random.uniform(-10, 10)
        speed = max(0, min(120, speed))  # 限制速度 0-120 km/h
        
        direction += random.uniform(-30, 30)
        direction = direction % 360
        
        yield (lng, lat, speed, direction, current_time)
        
        # 计算下一个位置
        if speed > 0:
            # 移动距离（km）
            distance_km = speed * interval_seconds / 3600
            
            # 转换为经纬度变化
            delta_lat = distance_km * math.cos(math.radians(direction)) / 111.0
            delta_lng = distance_km * math.sin(math.radians(direction)) / (111.0 * math.cos(math.radians(lat)))
            
            new_lat = lat + delta_lat
            new_lng = lng + delta_lng
            
            # 检查是否超出城市范围，如果超出则调转方向
            dist_to_center = math.sqrt(
                ((new_lng - city_center_lng) * 111 * math.cos(math.radians(lat))) ** 2 +
                ((new_lat - city_center_lat) * 111) ** 2
            )
            
            if dist_to_center > city_radius_km:
                # 调转方向，朝向城市中心
                direction = math.degrees(math.atan2(
                    city_center_lng - lng,
                    city_center_lat - lat
                ))
                direction = (direction + random.uniform(-30, 30)) % 360
            else:
                lng, lat = new_lng, new_lat
        
        # 更新时间
        current_time += timedelta(seconds=interval_seconds)


def generate_all_data(
    num_vehicles: int = 200,
    total_records: int = 10_000_000,
    start_date: datetime = None
) -> Generator[Tuple[str, float, float, float, float, datetime], None, None]:
    """
    生成所有车辆的轨迹数据
    
    Args:
        num_vehicles: 车辆数量
        total_records: 总记录数
        start_date: 起始日期
    
    Yields:
        (vehicle_id, lng, lat, speed, direction, recorded_at)
    """
    if start_date is None:
        start_date = datetime(2025, 1, 1)
    
    records_per_vehicle = total_records // num_vehicles
    
    print(f"生成数据: {num_vehicles} 辆车, 每辆 {records_per_vehicle:,} 条记录")
    print(f"总记录数: {total_records:,}")
    
    for i in range(num_vehicles):
        vehicle_id = generate_vehicle_id(i)
        
        # 随机分配到一个城市
        city = random.choice(CITY_CENTERS)
        city_name, city_lng, city_lat, city_radius = city
        
        # 随机起始位置（在城市范围内）
        angle = random.uniform(0, 2 * math.pi)
        dist = random.uniform(0, city_radius) / 111.0  # 转换为度
        start_lng = city_lng + dist * math.sin(angle) / math.cos(math.radians(city_lat))
        start_lat = city_lat + dist * math.cos(angle)
        
        # 随机起始时间（在起始日期后的30天内）
        start_time = start_date + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # 生成轨迹
        track_gen = simulate_vehicle_movement(
            start_lng, start_lat,
            city_radius, city_lng, city_lat,
            records_per_vehicle,
            start_time,
            interval_seconds=30
        )
        
        for lng, lat, speed, direction, recorded_at in track_gen:
            yield (vehicle_id, lng, lat, speed, direction, recorded_at)


def main():
    """主函数：生成数据并插入数据库"""
    NUM_VEHICLES = 200
    TOTAL_RECORDS = 10_000_000  # 1000万条
    BATCH_SIZE = 100_000  # 每批10万条
    
    print("=" * 60)
    print("车辆轨迹数据生成器")
    print("=" * 60)
    print(f"\n配置:")
    print(f"  车辆数量: {NUM_VEHICLES}")
    print(f"  总记录数: {TOTAL_RECORDS:,}")
    print(f"  批次大小: {BATCH_SIZE:,}")
    print()
    
    # 初始化数据库
    tracker = VehicleTracker()
    tracker.init_tables()
    
    # 询问是否清空现有数据
    stats = tracker.get_stats()
    if stats['track_count'] > 0:
        print(f"\n当前数据库已有 {stats['track_count']:,} 条轨迹记录")
        response = input("是否清空现有数据? (y/n): ").strip().lower()
        if response == 'y':
            tracker.clear_data()
    
    # 生成并插入车辆信息
    print("\n生成车辆信息...")
    vehicles = []
    for i in range(NUM_VEHICLES):
        city_idx = i % len(CITY_CENTERS)
        vehicles.append({
            'vehicle_id': generate_vehicle_id(i),
            'plate_number': generate_plate_number(city_idx),
            'vehicle_type': random.choice(['轿车', 'SUV', '货车', '面包车'])
        })
    tracker.insert_vehicles(vehicles)
    
    # 生成轨迹数据
    print("\n生成轨迹数据...")
    start_time = time.time()
    
    data_gen = generate_all_data(NUM_VEHICLES, TOTAL_RECORDS)
    
    batch = []
    total_inserted = 0
    
    for record in data_gen:
        batch.append(record)
        
        if len(batch) >= BATCH_SIZE:
            # 使用 COPY 命令快速插入
            tracker.insert_tracks_copy(batch)
            total_inserted += len(batch)
            
            elapsed = time.time() - start_time
            rate = total_inserted / elapsed
            remaining = (TOTAL_RECORDS - total_inserted) / rate if rate > 0 else 0
            
            print(f"进度: {total_inserted:,}/{TOTAL_RECORDS:,} "
                  f"({100*total_inserted/TOTAL_RECORDS:.1f}%) "
                  f"| 速度: {rate:,.0f} 条/秒 "
                  f"| 剩余: {remaining/60:.1f} 分钟")
            
            batch = []
    
    # 插入剩余数据
    if batch:
        tracker.insert_tracks_copy(batch)
        total_inserted += len(batch)
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("数据生成完成!")
    print("=" * 60)
    print(f"总记录数: {total_inserted:,}")
    print(f"总耗时: {elapsed:.1f} 秒")
    print(f"平均速度: {total_inserted/elapsed:,.0f} 条/秒")
    
    # 显示最终统计
    stats = tracker.get_stats()
    print(f"\n数据库统计:")
    print(f"  车辆数: {stats['vehicle_count']:,}")
    print(f"  轨迹数: {stats['track_count']:,}")
    if stats['geo_range']:
        print(f"  经度范围: {stats['geo_range']['lng'][0]:.3f} ~ {stats['geo_range']['lng'][1]:.3f}")
        print(f"  纬度范围: {stats['geo_range']['lat'][0]:.3f} ~ {stats['geo_range']['lat'][1]:.3f}")
    
    tracker.close()


if __name__ == '__main__':
    main()
