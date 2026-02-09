# -*- coding: utf-8 -*-
"""
中国行政区划边界数据加载模块
从 ok_geo.csv 加载省市区三级边界数据
数据来源: https://github.com/xiangyuecn/AreaCity-JsSpider-StatsGov
"""

import csv
import os
import sys
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

# 增加CSV字段大小限制（边界数据可能很大）
csv.field_size_limit(sys.maxsize)


@dataclass
class Region:
    """行政区域数据类"""
    id: int                          # 区域ID
    pid: int                         # 父级ID
    deep: int                        # 层级：0=省，1=市，2=区县
    name: str                        # 区域名称
    ext_path: str                    # 完整路径，如 "上海市 上海市 浦东新区"
    center: Optional[Tuple[float, float]]  # 中心坐标 (经度, 纬度)
    bbox: Optional[Tuple[float, float, float, float]]  # 边界框 (min_lon, min_lat, max_lon, max_lat)
    polygons: List[List[Tuple[float, float]]]  # 多边形边界列表


class GeoDataLoader:
    """地理数据加载器"""
    
    def __init__(self, csv_path: str = None):
        """
        初始化数据加载器
        
        Args:
            csv_path: CSV文件路径，默认为当前目录下的 ok_geo.csv
        """
        if csv_path is None:
            csv_path = os.path.join(os.path.dirname(__file__), 'ok_geo.csv')
        
        self.csv_path = csv_path
        self.regions: Dict[int, Region] = {}
        self.provinces: List[Region] = []      # deep=0
        self.cities: List[Region] = []         # deep=1
        self.districts: List[Region] = []      # deep=2
        self._loaded = False
    
    def _parse_center(self, geo_str: str) -> Optional[Tuple[float, float]]:
        """解析中心坐标"""
        if not geo_str or geo_str == 'EMPTY':
            return None
        try:
            parts = geo_str.strip().split(' ')
            if len(parts) == 2:
                return (float(parts[0]), float(parts[1]))  # (经度, 纬度)
        except (ValueError, IndexError):
            pass
        return None
    
    def _parse_polygon(self, polygon_str: str) -> Tuple[List[List[Tuple[float, float]]], Optional[Tuple[float, float, float, float]]]:
        """
        解析边界多边形数据
        
        格式: "lng lat,lng lat,...;lng lat,lng lat,..."
        多个地块用;分隔，每个地块的坐标点用,分隔
        
        Returns:
            (多边形列表, 边界框)
        """
        polygons = []
        if not polygon_str or polygon_str == 'EMPTY':
            return polygons, None
        
        min_lon = float('inf')
        min_lat = float('inf')
        max_lon = float('-inf')
        max_lat = float('-inf')
        
        try:
            # 处理可能存在的孔洞标记 ~
            parts = polygon_str.split(';')
            for part in parts:
                if not part.strip():
                    continue
                # 暂时忽略孔洞（以~开头的部分）
                if '~' in part:
                    sub_parts = part.split('~')
                    part = sub_parts[0]  # 只取外环
                
                points = []
                coords = part.split(',')
                for coord in coords:
                    coord = coord.strip()
                    if not coord:
                        continue
                    xy = coord.split(' ')
                    if len(xy) == 2:
                        lon, lat = float(xy[0]), float(xy[1])
                        points.append((lon, lat))
                        min_lon = min(min_lon, lon)
                        min_lat = min(min_lat, lat)
                        max_lon = max(max_lon, lon)
                        max_lat = max(max_lat, lat)
                
                if len(points) >= 3:
                    polygons.append(points)
        except (ValueError, IndexError) as e:
            pass
        
        bbox = None
        if polygons:
            bbox = (min_lon, min_lat, max_lon, max_lat)
        
        return polygons, bbox
    
    def load(self) -> None:
        """加载CSV数据"""
        if self._loaded:
            return
        
        print(f"正在加载地理数据: {self.csv_path}")
        
        with open(self.csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 6:
                    continue
                
                try:
                    region_id = int(row[0])
                    pid = int(row[1])
                    deep = int(row[2])
                    name = row[3]
                    ext_path = row[4]
                    geo = row[5] if len(row) > 5 else ''
                    polygon = row[6] if len(row) > 6 else ''
                    
                    center = self._parse_center(geo)
                    polygons, bbox = self._parse_polygon(polygon)
                    
                    region = Region(
                        id=region_id,
                        pid=pid,
                        deep=deep,
                        name=name,
                        ext_path=ext_path,
                        center=center,
                        bbox=bbox,
                        polygons=polygons
                    )
                    
                    self.regions[region_id] = region
                    
                    if deep == 0:
                        self.provinces.append(region)
                    elif deep == 1:
                        self.cities.append(region)
                    elif deep == 2:
                        self.districts.append(region)
                        
                except (ValueError, IndexError) as e:
                    continue
        
        self._loaded = True
        print(f"数据加载完成: {len(self.provinces)} 个省, {len(self.cities)} 个市, {len(self.districts)} 个区县")
    
    def get_region_by_name(self, name: str) -> Optional[Region]:
        """根据名称查找区域"""
        self.load()
        for region in self.regions.values():
            if name in region.name or name in region.ext_path:
                return region
        return None
    
    def get_children(self, parent_id: int) -> List[Region]:
        """获取下级区域"""
        self.load()
        return [r for r in self.regions.values() if r.pid == parent_id]


# 全局单例
_loader_instance = None


def get_loader() -> GeoDataLoader:
    """获取全局数据加载器实例"""
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = GeoDataLoader()
    return _loader_instance


if __name__ == '__main__':
    # 测试加载
    loader = get_loader()
    loader.load()
    
    # 查找浦东新区
    region = loader.get_region_by_name('浦东')
    if region:
        print(f"\n找到: {region.ext_path}")
        print(f"  ID: {region.id}")
        print(f"  中心坐标: {region.center}")
        print(f"  边界框: {region.bbox}")
        print(f"  多边形数量: {len(region.polygons)}")
