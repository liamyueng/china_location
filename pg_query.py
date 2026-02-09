#!/usr/bin/env python3
"""
使用 PostgreSQL + PostGIS 进行坐标查询
"""

import psycopg2
from typing import Optional, Dict, List
from dataclasses import dataclass


@dataclass
class LocationResult:
    """查询结果"""
    province: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    full_path: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            'province': self.province,
            'city': self.city,
            'district': self.district,
            'full_path': self.full_path
        }
    
    def __str__(self) -> str:
        parts = [p for p in [self.province, self.city, self.district] if p]
        return ' '.join(parts) if parts else '未找到'


class PGLocationQuery:
    """PostgreSQL 坐标查询类"""
    
    def __init__(self, 
                 host: str = 'localhost',
                 port: int = 5432,
                 database: str = 'china_geo',
                 user: str = 'postgres',
                 password: str = 'postgres'):
        """
        初始化数据库连接
        
        Args:
            host: 数据库主机
            port: 端口
            database: 数据库名
            user: 用户名
            password: 密码
        """
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        self._verify_connection()
    
    def _verify_connection(self):
        """验证数据库连接和数据"""
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM regions")
        count = cur.fetchone()[0]
        cur.close()
        if count == 0:
            raise ValueError("数据库中没有数据，请先运行 import_to_postgresql.py")
        print(f"已连接数据库，共 {count} 条区域数据")
    
    def find_location(self, lng: float, lat: float) -> Dict[str, Optional[str]]:
        """
        根据经纬度查询所属行政区划
        
        Args:
            lng: 经度（GCJ-02 坐标系）
            lat: 纬度（GCJ-02 坐标系）
        
        Returns:
            包含 province, city, district 的字典
        """
        result = LocationResult()
        
        cur = self.conn.cursor()
        
        # 使用 PostGIS 的 ST_Contains 函数查询
        cur.execute("""
            SELECT name, ext_path, deep
            FROM regions
            WHERE ST_Contains(polygon, ST_SetSRID(ST_Point(%s, %s), 4326))
            ORDER BY deep
        """, (lng, lat))
        
        rows = cur.fetchall()
        cur.close()
        
        for name, ext_path, deep in rows:
            if deep == 0:
                result.province = name
            elif deep == 1:
                result.city = name
            elif deep == 2:
                result.district = name
                result.full_path = ext_path
        
        return result.to_dict()
    
    def find_location_detail(self, lng: float, lat: float) -> List[Dict]:
        """
        查询坐标所属的所有行政区划（详细信息）
        
        Args:
            lng: 经度
            lat: 纬度
        
        Returns:
            所有匹配区域的详细信息列表
        """
        cur = self.conn.cursor()
        
        cur.execute("""
            SELECT id, name, ext_path, deep, center_lng, center_lat,
                   ST_Distance(
                       ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
                       ST_SetSRID(ST_Point(center_lng, center_lat), 4326)::geography
                   ) as distance_to_center
            FROM regions
            WHERE ST_Contains(polygon, ST_SetSRID(ST_Point(%s, %s), 4326))
            ORDER BY deep
        """, (lng, lat, lng, lat))
        
        results = []
        for row in cur.fetchall():
            results.append({
                'id': row[0],
                'name': row[1],
                'path': row[2],
                'level': row[3],
                'center': (row[4], row[5]),
                'distance_to_center_m': round(row[6], 2) if row[6] else None
            })
        
        cur.close()
        return results
    
    def find_by_name(self, name: str) -> List[Dict]:
        """
        按名称搜索区域
        
        Args:
            name: 区域名称（支持模糊匹配）
        
        Returns:
            匹配的区域列表
        """
        cur = self.conn.cursor()
        
        cur.execute("""
            SELECT id, name, ext_path, deep, center_lng, center_lat
            FROM regions
            WHERE name LIKE %s
            ORDER BY deep, name
        """, (f'%{name}%',))
        
        results = []
        for row in cur.fetchall():
            results.append({
                'id': row[0],
                'name': row[1],
                'path': row[2],
                'level': row[3],
                'center': (row[4], row[5])
            })
        
        cur.close()
        return results
    
    def get_children(self, parent_id: int) -> List[Dict]:
        """
        获取下级行政区划
        
        Args:
            parent_id: 父区域 ID
        
        Returns:
            子区域列表
        """
        cur = self.conn.cursor()
        
        cur.execute("""
            SELECT id, name, ext_path, deep, center_lng, center_lat
            FROM regions
            WHERE pid = %s
            ORDER BY id
        """, (parent_id,))
        
        results = []
        for row in cur.fetchall():
            results.append({
                'id': row[0],
                'name': row[1],
                'path': row[2],
                'level': row[3],
                'center': (row[4], row[5])
            })
        
        cur.close()
        return results
    
    def batch_find(self, coordinates: List[tuple]) -> List[Dict]:
        """
        批量查询多个坐标
        
        Args:
            coordinates: [(lng1, lat1), (lng2, lat2), ...] 坐标列表
        
        Returns:
            对应的查询结果列表
        """
        results = []
        for lng, lat in coordinates:
            result = self.find_location(lng, lat)
            result['coordinate'] = (lng, lat)
            results.append(result)
        return results
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# 便捷函数
_default_query = None

def get_query() -> PGLocationQuery:
    """获取默认查询实例（单例）"""
    global _default_query
    if _default_query is None:
        _default_query = PGLocationQuery()
    return _default_query


def find_location(lng: float, lat: float) -> Dict[str, Optional[str]]:
    """
    便捷函数：根据经纬度查询所属行政区划
    
    Args:
        lng: 经度（GCJ-02 坐标系）
        lat: 纬度（GCJ-02 坐标系）
    
    Returns:
        包含 province, city, district 的字典
    
    Example:
        >>> from pg_query import find_location
        >>> find_location(121.544, 31.221)
        {'province': '上海市', 'city': '上海市', 'district': '浦东新区', 'full_path': '...'}
    """
    return get_query().find_location(lng, lat)


# 测试
if __name__ == '__main__':
    print("=== PostgreSQL 坐标查询测试 ===\n")
    
    try:
        query = PGLocationQuery()
        
        # 测试查询
        test_coords = [
            (121.544, 31.221),   # 浦东新区中心
            (116.407, 39.904),   # 北京天安门
            (121.474, 31.230),   # 上海
            (113.264, 23.129),   # 广州
            (114.057, 22.543),   # 深圳
        ]
        
        print("坐标查询测试:")
        print("-" * 60)
        for lng, lat in test_coords:
            result = query.find_location(lng, lat)
            province = result.get('province', '?')
            city = result.get('city', '?')
            district = result.get('district', '?')
            print(f"  ({lng}, {lat}) => {province} {city} {district}")
        
        # 测试按名称搜索
        print("\n按名称搜索 '浦东':")
        print("-" * 60)
        for item in query.find_by_name('浦东'):
            print(f"  {item['name']} - {item['path']}")
        
        # 测试详细查询
        print("\n详细查询 (121.544, 31.221):")
        print("-" * 60)
        for item in query.find_location_detail(121.544, 31.221):
            level_name = ['省', '市', '区县'][item['level']] if item['level'] < 3 else f"L{item['level']}"
            print(f"  [{level_name}] {item['name']}, 距中心 {item['distance_to_center_m']}米")
        
        query.close()
        
    except Exception as e:
        print(f"错误: {e}")
        print("\n请确保已运行 import_to_postgresql.py 导入数据")
