#!/usr/bin/env python3
"""
车辆轨迹数据管理系统
支持:
1. 创建数据库表
2. 批量插入轨迹数据
3. 圆形范围查询（给定经纬度和半径）
"""

import psycopg2
from psycopg2 import extras
import math
from typing import List, Dict, Tuple, Optional
from datetime import datetime


class VehicleTracker:
    """车辆轨迹管理类"""
    
    # 地球半径（米）
    EARTH_RADIUS = 6371000
    
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 5432,
                 database: str = 'vehicle_tracker',
                 user: str = 'postgres',
                 password: str = 'postgres'):
        """初始化数据库连接"""
        self.db_config = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.conn = None
        self._connect()
    
    def _connect(self):
        """连接数据库"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
        except psycopg2.OperationalError:
            # 数据库不存在，先创建
            self._create_database()
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
    
    def _create_database(self):
        """创建数据库"""
        config = self.db_config.copy()
        db_name = config.pop('database')
        config['database'] = 'postgres'
        
        conn = psycopg2.connect(**config)
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        if not cur.fetchone():
            cur.execute(f'CREATE DATABASE {db_name}')
            print(f"数据库 {db_name} 已创建")
        
        cur.close()
        conn.close()
    
    def init_tables(self):
        """初始化数据表"""
        cur = self.conn.cursor()
        
        # 创建车辆表
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vehicles (
                id SERIAL PRIMARY KEY,
                vehicle_id VARCHAR(50) UNIQUE NOT NULL,
                plate_number VARCHAR(20),
                vehicle_type VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建轨迹表（核心表）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                id BIGSERIAL PRIMARY KEY,
                vehicle_id VARCHAR(50) NOT NULL,
                lng DOUBLE PRECISION NOT NULL,
                lat DOUBLE PRECISION NOT NULL,
                speed REAL,
                direction REAL,
                recorded_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建索引以加速查询
        # 经纬度复合索引（用于范围查询）
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tracks_lng_lat 
            ON tracks (lng, lat)
        """)
        
        # 车辆ID索引
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tracks_vehicle_id 
            ON tracks (vehicle_id)
        """)
        
        # 时间索引
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tracks_recorded_at 
            ON tracks (recorded_at)
        """)
        
        # 复合索引（经纬度+时间）
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tracks_lng_lat_time 
            ON tracks (lng, lat, recorded_at)
        """)
        
        self.conn.commit()
        cur.close()
        print("数据表初始化完成")
    
    def insert_vehicles(self, vehicles: List[Dict]):
        """批量插入车辆信息"""
        cur = self.conn.cursor()
        
        sql = """
            INSERT INTO vehicles (vehicle_id, plate_number, vehicle_type)
            VALUES (%s, %s, %s)
            ON CONFLICT (vehicle_id) DO NOTHING
        """
        
        data = [(v['vehicle_id'], v.get('plate_number'), v.get('vehicle_type')) 
                for v in vehicles]
        
        extras.execute_batch(cur, sql, data, page_size=1000)
        self.conn.commit()
        cur.close()
        print(f"插入 {len(vehicles)} 辆车辆信息")
    
    def insert_tracks_batch(self, tracks: List[Tuple], batch_size: int = 10000):
        """
        批量插入轨迹数据
        
        Args:
            tracks: [(vehicle_id, lng, lat, speed, direction, recorded_at), ...]
            batch_size: 每批插入数量
        """
        cur = self.conn.cursor()
        
        sql = """
            INSERT INTO tracks (vehicle_id, lng, lat, speed, direction, recorded_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        total = len(tracks)
        inserted = 0
        
        for i in range(0, total, batch_size):
            batch = tracks[i:i + batch_size]
            extras.execute_batch(cur, sql, batch, page_size=batch_size)
            self.conn.commit()
            inserted += len(batch)
            
            if inserted % 100000 == 0 or inserted == total:
                print(f"已插入: {inserted:,}/{total:,} ({100*inserted/total:.1f}%)")
        
        cur.close()
        return inserted
    
    def insert_tracks_copy(self, tracks: List[Tuple]):
        """
        使用 COPY 命令快速插入（最快方式）
        
        Args:
            tracks: [(vehicle_id, lng, lat, speed, direction, recorded_at), ...]
        """
        import io
        
        cur = self.conn.cursor()
        
        # 转换为 CSV 格式
        buffer = io.StringIO()
        for t in tracks:
            line = '\t'.join([
                str(t[0]),  # vehicle_id
                str(t[1]),  # lng
                str(t[2]),  # lat
                str(t[3]) if t[3] is not None else '\\N',  # speed
                str(t[4]) if t[4] is not None else '\\N',  # direction
                t[5].isoformat() if isinstance(t[5], datetime) else str(t[5])  # recorded_at
            ])
            buffer.write(line + '\n')
        
        buffer.seek(0)
        
        cur.copy_from(
            buffer,
            'tracks',
            columns=('vehicle_id', 'lng', 'lat', 'speed', 'direction', 'recorded_at'),
            null='\\N'
        )
        
        self.conn.commit()
        cur.close()
        print(f"COPY 插入 {len(tracks):,} 条记录")
    
    @staticmethod
    def _calculate_bbox(lng: float, lat: float, radius_m: float) -> Tuple[float, float, float, float]:
        """
        计算圆形范围的边界框（用于快速筛选）
        
        Args:
            lng: 中心经度
            lat: 中心纬度
            radius_m: 半径（米）
        
        Returns:
            (min_lng, max_lng, min_lat, max_lat)
        """
        # 1度纬度约111km
        lat_delta = radius_m / 111000.0
        
        # 1度经度的距离随纬度变化
        lng_delta = radius_m / (111000.0 * math.cos(math.radians(lat)))
        
        return (
            lng - lng_delta,  # min_lng
            lng + lng_delta,  # max_lng
            lat - lat_delta,  # min_lat
            lat + lat_delta   # max_lat
        )
    
    @staticmethod
    def _haversine_distance(lng1: float, lat1: float, lng2: float, lat2: float) -> float:
        """
        计算两点间的球面距离（Haversine 公式）
        
        Returns:
            距离（米）
        """
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lng = math.radians(lng2 - lng1)
        
        a = math.sin(delta_lat / 2) ** 2 + \
            math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lng / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return VehicleTracker.EARTH_RADIUS * c
    
    def find_in_circle(self, 
                       lng: float, 
                       lat: float, 
                       radius_m: float,
                       start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None,
                       vehicle_id: Optional[str] = None,
                       limit: int = 1000) -> List[Dict]:
        """
        圆形范围查询 - 查找指定圆形区域内的所有轨迹点
        
        Args:
            lng: 圆心经度
            lat: 圆心纬度
            radius_m: 半径（米）
            start_time: 开始时间（可选）
            end_time: 结束时间（可选）
            vehicle_id: 车辆ID（可选，过滤特定车辆）
            limit: 返回数量限制
        
        Returns:
            轨迹点列表，每个点包含距离信息
        """
        # 计算边界框
        min_lng, max_lng, min_lat, max_lat = self._calculate_bbox(lng, lat, radius_m)
        
        cur = self.conn.cursor()
        
        # 构建查询（先用边界框快速筛选）
        sql = """
            SELECT id, vehicle_id, lng, lat, speed, direction, recorded_at
            FROM tracks
            WHERE lng BETWEEN %s AND %s
              AND lat BETWEEN %s AND %s
        """
        params = [min_lng, max_lng, min_lat, max_lat]
        
        # 可选过滤条件
        if start_time:
            sql += " AND recorded_at >= %s"
            params.append(start_time)
        
        if end_time:
            sql += " AND recorded_at <= %s"
            params.append(end_time)
        
        if vehicle_id:
            sql += " AND vehicle_id = %s"
            params.append(vehicle_id)
        
        sql += f" LIMIT {limit * 2}"  # 多取一些，因为还要过滤圆形
        
        cur.execute(sql, params)
        candidates = cur.fetchall()
        cur.close()
        
        # 精确计算距离，过滤出圆形范围内的点
        results = []
        for row in candidates:
            track_id, vid, track_lng, track_lat, speed, direction, recorded_at = row
            
            # 计算实际距离
            distance = self._haversine_distance(lng, lat, track_lng, track_lat)
            
            if distance <= radius_m:
                results.append({
                    'id': track_id,
                    'vehicle_id': vid,
                    'lng': track_lng,
                    'lat': track_lat,
                    'speed': speed,
                    'direction': direction,
                    'recorded_at': recorded_at,
                    'distance_m': round(distance, 2)
                })
                
                if len(results) >= limit:
                    break
        
        # 按距离排序
        results.sort(key=lambda x: x['distance_m'])
        
        return results
    
    def count_in_circle(self,
                        lng: float,
                        lat: float,
                        radius_m: float,
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None) -> int:
        """
        统计圆形范围内的记录数量
        """
        min_lng, max_lng, min_lat, max_lat = self._calculate_bbox(lng, lat, radius_m)
        
        cur = self.conn.cursor()
        
        # 使用边界框快速估算（稍微多于实际圆形范围）
        sql = """
            SELECT COUNT(*)
            FROM tracks
            WHERE lng BETWEEN %s AND %s
              AND lat BETWEEN %s AND %s
        """
        params = [min_lng, max_lng, min_lat, max_lat]
        
        if start_time:
            sql += " AND recorded_at >= %s"
            params.append(start_time)
        
        if end_time:
            sql += " AND recorded_at <= %s"
            params.append(end_time)
        
        cur.execute(sql, params)
        count = cur.fetchone()[0]
        cur.close()
        
        # 边界框内的数量（圆形约为边界框的 π/4 ≈ 0.785）
        return count
    
    def get_vehicle_track(self,
                          vehicle_id: str,
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None,
                          limit: int = 1000) -> List[Dict]:
        """获取特定车辆的轨迹"""
        cur = self.conn.cursor()
        
        sql = """
            SELECT id, lng, lat, speed, direction, recorded_at
            FROM tracks
            WHERE vehicle_id = %s
        """
        params = [vehicle_id]
        
        if start_time:
            sql += " AND recorded_at >= %s"
            params.append(start_time)
        
        if end_time:
            sql += " AND recorded_at <= %s"
            params.append(end_time)
        
        sql += " ORDER BY recorded_at LIMIT %s"
        params.append(limit)
        
        cur.execute(sql, params)
        
        results = []
        for row in cur.fetchall():
            results.append({
                'id': row[0],
                'lng': row[1],
                'lat': row[2],
                'speed': row[3],
                'direction': row[4],
                'recorded_at': row[5]
            })
        
        cur.close()
        return results
    
    def get_stats(self) -> Dict:
        """获取数据库统计信息"""
        cur = self.conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM vehicles")
        vehicle_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM tracks")
        track_count = cur.fetchone()[0]
        
        cur.execute("""
            SELECT MIN(recorded_at), MAX(recorded_at) 
            FROM tracks
        """)
        time_range = cur.fetchone()
        
        cur.execute("""
            SELECT MIN(lng), MAX(lng), MIN(lat), MAX(lat)
            FROM tracks
        """)
        geo_range = cur.fetchone()
        
        cur.close()
        
        return {
            'vehicle_count': vehicle_count,
            'track_count': track_count,
            'time_range': {
                'start': time_range[0],
                'end': time_range[1]
            } if time_range[0] else None,
            'geo_range': {
                'lng': (geo_range[0], geo_range[1]),
                'lat': (geo_range[2], geo_range[3])
            } if geo_range[0] else None
        }
    
    def clear_data(self):
        """清空所有数据"""
        cur = self.conn.cursor()
        cur.execute("TRUNCATE TABLE tracks RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE vehicles RESTART IDENTITY CASCADE")
        self.conn.commit()
        cur.close()
        print("数据已清空")
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# 便捷函数
def find_in_circle(lng: float, lat: float, radius_m: float, **kwargs) -> List[Dict]:
    """
    便捷函数：圆形范围查询
    
    Args:
        lng: 圆心经度
        lat: 圆心纬度
        radius_m: 半径（米）
        start_time: 开始时间（可选）
        end_time: 结束时间（可选）
        vehicle_id: 车辆ID（可选）
        limit: 返回数量限制（默认1000）
    
    Returns:
        轨迹点列表
    
    Example:
        >>> from vehicle_tracker import find_in_circle
        >>> results = find_in_circle(116.407, 39.904, 1000)  # 天安门1公里范围
        >>> for r in results[:5]:
        ...     print(f"{r['vehicle_id']}: {r['distance_m']}m")
    """
    with VehicleTracker() as tracker:
        return tracker.find_in_circle(lng, lat, radius_m, **kwargs)


if __name__ == '__main__':
    print("=== 车辆轨迹数据管理系统 ===\n")
    
    tracker = VehicleTracker()
    tracker.init_tables()
    
    stats = tracker.get_stats()
    print(f"\n当前数据统计:")
    print(f"  车辆数: {stats['vehicle_count']:,}")
    print(f"  轨迹数: {stats['track_count']:,}")
    
    if stats['time_range']:
        print(f"  时间范围: {stats['time_range']['start']} ~ {stats['time_range']['end']}")
    
    if stats['geo_range']:
        print(f"  经度范围: {stats['geo_range']['lng']}")
        print(f"  纬度范围: {stats['geo_range']['lat']}")
    
    tracker.close()
