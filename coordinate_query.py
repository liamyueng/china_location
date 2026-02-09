# -*- coding: utf-8 -*-
"""
基于多边形边界的坐标查询模块
精确到区县级别的坐标反查
"""

from typing import List, Tuple, Optional, Dict
from geo_data_loader import get_loader, Region, GeoDataLoader


def point_in_polygon(lon: float, lat: float, polygon: List[Tuple[float, float]]) -> bool:
    """
    使用射线法（Ray Casting）判断点是否在多边形内
    
    原理：从该点向右发射一条水平射线，计算与多边形边界的交点数
    - 交点数为奇数：点在多边形内
    - 交点数为偶数：点在多边形外
    
    Args:
        lon: 经度
        lat: 纬度
        polygon: 多边形顶点列表 [(lon1, lat1), (lon2, lat2), ...]
    
    Returns:
        True 如果点在多边形内，否则 False
    """
    n = len(polygon)
    if n < 3:
        return False
    
    inside = False
    
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        
        # 检查射线是否穿过边 (i, j)
        if ((yi > lat) != (yj > lat)) and \
           (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi):
            inside = not inside
        
        j = i
    
    return inside


def point_in_bbox(lon: float, lat: float, bbox: Tuple[float, float, float, float]) -> bool:
    """
    快速判断点是否在边界框内
    
    Args:
        lon: 经度
        lat: 纬度
        bbox: (min_lon, min_lat, max_lon, max_lat)
    
    Returns:
        True 如果点在边界框内
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat


def point_in_region(lon: float, lat: float, region: Region) -> bool:
    """
    判断点是否在指定区域内
    
    Args:
        lon: 经度
        lat: 纬度
        region: 区域对象
    
    Returns:
        True 如果点在区域内
    """
    # 首先用边界框快速排除
    if region.bbox is None:
        return False
    
    if not point_in_bbox(lon, lat, region.bbox):
        return False
    
    # 精确判断是否在多边形内
    for polygon in region.polygons:
        if point_in_polygon(lon, lat, polygon):
            return True
    
    return False


class CoordinateQuery:
    """坐标查询类"""
    
    def __init__(self, loader: GeoDataLoader = None):
        """
        初始化查询器
        
        Args:
            loader: 数据加载器，如果为None则使用全局单例
        """
        self.loader = loader or get_loader()
        self.loader.load()
    
    def query(self, lon: float, lat: float) -> Dict:
        """
        根据经纬度查询所在的省市区
        
        Args:
            lon: 经度 (东经为正)
            lat: 纬度 (北纬为正)
        
        Returns:
            {
                'province': 省名,
                'city': 市名,
                'district': 区县名,
                'full_path': 完整路径,
                'region': Region对象
            }
            如果未找到，对应字段为 None
        """
        result = {
            'province': None,
            'city': None,
            'district': None,
            'full_path': None,
            'region': None
        }
        
        # 策略: 先查区县（最精确），然后逐级向上
        # 1. 首先在区县级别查找
        for district in self.loader.districts:
            if point_in_region(lon, lat, district):
                result['district'] = district.name
                result['full_path'] = district.ext_path
                result['region'] = district
                
                # 解析完整路径获取省市
                parts = district.ext_path.split()
                if len(parts) >= 1:
                    result['province'] = parts[0]
                if len(parts) >= 2:
                    result['city'] = parts[1]
                
                return result
        
        # 2. 如果区县没找到，在市级别查找
        for city in self.loader.cities:
            if point_in_region(lon, lat, city):
                result['city'] = city.name
                result['full_path'] = city.ext_path
                result['region'] = city
                
                parts = city.ext_path.split()
                if len(parts) >= 1:
                    result['province'] = parts[0]
                
                return result
        
        # 3. 最后在省级别查找
        for province in self.loader.provinces:
            if point_in_region(lon, lat, province):
                result['province'] = province.name
                result['full_path'] = province.ext_path
                result['region'] = province
                return result
        
        return result
    
    def query_district(self, lon: float, lat: float) -> Optional[str]:
        """
        快速查询区县名
        
        Args:
            lon: 经度
            lat: 纬度
        
        Returns:
            区县名，未找到返回 None
        """
        result = self.query(lon, lat)
        return result['district']
    
    def query_full(self, lon: float, lat: float) -> Optional[str]:
        """
        查询完整路径
        
        Args:
            lon: 经度
            lat: 纬度
        
        Returns:
            完整路径如 "上海市 上海市 黄浦区"，未找到返回 None
        """
        result = self.query(lon, lat)
        return result['full_path']


def dms_to_decimal(degrees: int, minutes: int, seconds: float = 0) -> float:
    """
    度分秒转十进制度
    
    Args:
        degrees: 度
        minutes: 分
        seconds: 秒 (可选)
    
    Returns:
        十进制度数
    
    Example:
        >>> dms_to_decimal(110, 3, 0)  # 110°3′
        110.05
    """
    return degrees + minutes / 60 + seconds / 3600


# 便捷函数
_query_instance = None


def get_query() -> CoordinateQuery:
    """获取全局查询器实例"""
    global _query_instance
    if _query_instance is None:
        _query_instance = CoordinateQuery()
    return _query_instance


def query_location(lon: float, lat: float) -> Dict:
    """
    查询坐标所在的省市区
    
    Args:
        lon: 经度
        lat: 纬度
    
    Returns:
        包含省市区信息的字典
    """
    return get_query().query(lon, lat)


def query_district(lon: float, lat: float) -> Optional[str]:
    """查询区县名"""
    return get_query().query_district(lon, lat)


if __name__ == '__main__':
    # 测试
    print("=" * 60)
    print("坐标查询测试")
    print("=" * 60)
    
    # 测试坐标
    test_coords = [
        (116.4074, 39.9042, "北京天安门附近"),
        (121.4737, 31.2304, "上海人民广场附近"),
        (113.2644, 23.1291, "广州"),
        (110.5, 22.9, "黄浦区范围内测试点"),
    ]
    
    query = CoordinateQuery()
    
    for lon, lat, desc in test_coords:
        result = query.query(lon, lat)
        print(f"\n{desc}: ({lon}, {lat})")
        print(f"  省份: {result['province']}")
        print(f"  城市: {result['city']}")
        print(f"  区县: {result['district']}")
        print(f"  完整: {result['full_path']}")
