#!/usr/bin/env python3
"""
使用 PostgreSQL 进行坐标查询（不使用 PostGIS）
使用边界框快速筛选 + Python 射线法精确判断
"""

import json
import psycopg2
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass


def point_in_polygon(lng: float, lat: float, polygon: List[List[float]]) -> bool:
    """
    使用射线法判断点是否在多边形内
    
    Args:
        lng: 经度
        lat: 纬度
        polygon: 多边形顶点列表 [[lng, lat], ...]
    
    Returns:
        True 如果点在多边形内
    """
    n = len(polygon)
    inside = False
    
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        
        if ((yi > lat) != (yj > lat)) and \
           (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi):
            inside = not inside
        
        j = i
    
    return inside


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


class PGSimpleQuery:
    """PostgreSQL 坐标查询类（不使用 PostGIS）"""
    
    def __init__(self, 
                 host: str = 'localhost',
                 port: int = 5432,
                 database: str = 'china_geo_simple',
                 user: str = 'postgres',
                 password: str = 'postgres'):
        """
        初始化数据库连接
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
            raise ValueError("数据库中没有数据，请先运行 import_to_pg_simple.py")
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
        
        # 使用边界框快速筛选候选区域
        cur.execute("""
            SELECT id, name, ext_path, deep, polygon_json
            FROM regions
            WHERE bbox_min_lng <= %s AND bbox_max_lng >= %s
              AND bbox_min_lat <= %s AND bbox_max_lat >= %s
            ORDER BY deep
        """, (lng, lng, lat, lat))
        
        candidates = cur.fetchall()
        cur.close()
        
        # 使用射线法精确判断
        for region_id, name, ext_path, deep, polygon_json in candidates:
            polygon = json.loads(polygon_json)
            if point_in_polygon(lng, lat, polygon):
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
        """
        cur = self.conn.cursor()
        
        # 边界框筛选
        cur.execute("""
            SELECT id, name, ext_path, deep, center_lng, center_lat, polygon_json
            FROM regions
            WHERE bbox_min_lng <= %s AND bbox_max_lng >= %s
              AND bbox_min_lat <= %s AND bbox_max_lat >= %s
            ORDER BY deep
        """, (lng, lng, lat, lat))
        
        candidates = cur.fetchall()
        cur.close()
        
        results = []
        for region_id, name, ext_path, deep, clng, clat, polygon_json in candidates:
            polygon = json.loads(polygon_json)
            if point_in_polygon(lng, lat, polygon):
                # 计算到中心点的距离（简化计算，不考虑地球曲率）
                dist = ((lng - clng) ** 2 + (lat - clat) ** 2) ** 0.5 * 111000  # 约111km每度
                results.append({
                    'id': region_id,
                    'name': name,
                    'path': ext_path,
                    'level': deep,
                    'center': (clng, clat),
                    'distance_approx_m': round(dist, 2)
                })
        
        return results
    
    def find_by_name(self, name: str) -> List[Dict]:
        """按名称搜索区域"""
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
    
    def find_nearby(self, lng: float, lat: float, level: int = 2, limit: int = 5) -> List[Dict]:
        """
        查找附近的区域
        
        Args:
            lng: 经度
            lat: 纬度
            level: 行政级别 (0=省, 1=市, 2=区县)
            limit: 返回数量
        """
        cur = self.conn.cursor()
        
        # 使用简单的欧几里得距离排序（对于中国范围内足够准确）
        cur.execute("""
            SELECT id, name, ext_path, center_lng, center_lat,
                   SQRT(POWER(center_lng - %s, 2) + POWER(center_lat - %s, 2)) * 111000 AS dist
            FROM regions
            WHERE deep = %s
            ORDER BY dist
            LIMIT %s
        """, (lng, lat, level, limit))
        
        results = []
        for row in cur.fetchall():
            results.append({
                'id': row[0],
                'name': row[1],
                'path': row[2],
                'center': (row[3], row[4]),
                'distance_approx_m': round(row[5], 2)
            })
        
        cur.close()
        return results
    
    def get_children(self, parent_id: int) -> List[Dict]:
        """获取下级行政区划"""
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
    
    def batch_find(self, coordinates: List[Tuple[float, float]]) -> List[Dict]:
        """批量查询多个坐标"""
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

def get_query() -> PGSimpleQuery:
    """获取默认查询实例（单例）"""
    global _default_query
    if _default_query is None:
        _default_query = PGSimpleQuery()
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
        >>> from pg_simple_query import find_location
        >>> find_location(121.484, 31.232)
        {'province': '上海市', 'city': '上海市', 'district': '黄浦区', ...}
    """
    return get_query().find_location(lng, lat)


# 测试
if __name__ == '__main__':
    print("=== PostgreSQL 坐标查询测试 (无 PostGIS) ===\n")
    
    try:
        query = PGSimpleQuery()
        
        # 测试查询
        test_coords = [
            (121.484, 31.232),   # 黄浦区中心
            (116.407, 39.904),   # 北京天安门
            (121.474, 31.230),   # 上海
            (113.264, 23.129),   # 广州
            (114.057, 22.543),   # 深圳
        ]
        
        print("坐标查询测试:")
        print("-" * 60)
        for lng, lat in test_coords:
            result = query.find_location(lng, lat)
            province = result.get('province', '-')
            city = result.get('city', '-')
            district = result.get('district', '-')
            print(f"  ({lng}, {lat}) => {province} {city} {district}")
        
        # 测试按名称搜索
        print("\n按名称搜索 '黄浦':")
        print("-" * 60)
        for item in query.find_by_name('黄浦'):
            print(f"  {item['name']} - {item['path']}")
        
        # 测试查找附近
        print("\n查找 (121.484, 31.232) 附近的区县:")
        print("-" * 60)
        for item in query.find_nearby(121.484, 31.232, level=2, limit=5):
            print(f"  {item['name']}: {item['distance_approx_m']/1000:.2f} km ({item['path']})")
        
        query.close()
        
    except Exception as e:
        print(f"错误: {e}")
        print("\n请确保已运行 import_to_pg_simple.py 导入数据")
