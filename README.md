# 中国行政区划坐标查询 & 车辆轨迹圆形范围查询

两大功能：
1. 根据经纬度坐标查询所属的省/市/区县，精确到区县级别
2. 车辆轨迹数据管理，支持圆形范围查询（1000万条数据毫秒级响应）

## 数据来源

- 行政区划数据：[xiangyuecn/AreaCity-JsSpider-StatsGov](https://github.com/xiangyuecn/AreaCity-JsSpider-StatsGov)
- 坐标系：GCJ-02（高德地图坐标系）
- 数据文件：`ok_geo.csv`（约 152MB）

---

## 功能一：行政区划查询

### 1. 纯 Python 方式（无需数据库）

使用射线法（Ray Casting）判断点是否在多边形内：

```python
from coordinate_query import CoordinateQuery

query = CoordinateQuery("ok_geo.csv")
result = query.find_location(110.995, 22.918)
print(result)
# {'province': '广西壮族自治区', 'city': '梧州市', 'district': '岑溪市'}
```

### 2. PostgreSQL + PostGIS 方式（高性能）

使用空间数据库进行高效查询：

```python
from pg_query import find_location

result = find_location(110.995, 22.918)
# {'province': None, 'city': '梧州市', 'district': '岑溪市', 'full_path': '...'}
```

### 3. PostgreSQL 无 PostGIS 方式（轻量级）

使用边界框快速筛选 + Python 射线法精确判断：

```python
from pg_simple_query import find_location

result = find_location(110.995, 22.918)
# {'province': None, 'city': '梧州市', 'district': '岑溪市', 'full_path': '...'}
```

#### PostgreSQL 安装和设置

**方式一：使用 PostGIS**
```bash
sudo apt install postgresql postgresql-16-postgis-3
pip install psycopg2-binary
python import_to_postgresql.py
```

**方式二：不使用 PostGIS（推荐）**
```bash
sudo apt install postgresql
pip install psycopg2-binary
python import_to_pg_simple.py
```

3. 直接使用 SQL 查询：
```sql
-- 查询坐标所在区域
SELECT name, ext_path, deep
FROM regions
WHERE ST_Contains(polygon, ST_SetSRID(ST_Point(110.995, 22.918), 4326))
ORDER BY deep DESC;

-- 查询距离某点最近的区县
SELECT name, 
       ST_Distance(
           ST_SetSRID(ST_Point(110.995, 22.918), 4326)::geography,
           ST_SetSRID(ST_Point(center_lng, center_lat), 4326)::geography
       ) AS dist_meters
FROM regions
WHERE deep = 2
ORDER BY dist_meters
LIMIT 5;
```

---

## 功能二：车辆轨迹圆形范围查询

支持 1000 万条轨迹数据的高效查询，毫秒级响应。

### 安装

```bash
pip install psycopg2-binary
```

### 初始化数据库

```bash
# 生成 200 辆车的 1000 万条轨迹数据
python generate_vehicle_data.py
```

### 圆形范围查询

```python
from vehicle_tracker import VehicleTracker, find_in_circle

# 方式1: 便捷函数
results = find_in_circle(116.407, 39.904, 1000)  # 北京天安门1公里范围
for r in results[:5]:
    print(f"车辆 {r['vehicle_id']}: {r['distance_m']:.0f}m")

# 方式2: 使用类（更多功能）
from datetime import datetime

with VehicleTracker() as tracker:
    # 圆形范围查询，可指定时间范围
    results = tracker.find_in_circle(
        lng=116.407,
        lat=39.904,
        radius_m=1000,          # 半径（米）
        start_time=datetime(2025, 1, 1),
        end_time=datetime(2025, 1, 31),
        vehicle_id="V0001",     # 可选：指定车辆
        limit=100
    )
    
    # 统计范围内记录数
    count = tracker.count_in_circle(116.407, 39.904, 1000)
    
    # 获取特定车辆轨迹
    tracks = tracker.get_vehicle_track("V0001", limit=100)
```

### 性能测试结果

| 查询范围 | 数据量 | 查询时间 |
|---------|-------|---------|
| 500m 半径 | ~150条 | 2-3ms |
| 1km 半径 | ~600条 | 2-3ms |
| 5km 半径 | ~15000条 | 10-15ms |
| 10km 半径 | ~60000条 | 20-30ms |

**QPS**: ~1300+ (2km 半径随机查询)

---

## 文件说明

### 行政区划查询

| 文件 | 说明 |
|------|------|
| `ok_geo.csv` | 行政区划边界数据（需下载） |
| `geo_data_loader.py` | 纯 Python 数据加载模块 |
| `coordinate_query.py` | 纯 Python 坐标查询模块 |
| `import_to_postgresql.py` | PostgreSQL+PostGIS 导入脚本 |
| `import_to_pg_simple.py` | PostgreSQL（无PostGIS）导入脚本 |
| `pg_query.py` | PostgreSQL+PostGIS 查询模块 |
| `pg_simple_query.py` | PostgreSQL（无PostGIS）查询模块 |

### 车辆轨迹查询

| 文件 | 说明 |
|------|------|
| `vehicle_tracker.py` | 车辆轨迹管理和查询模块 |
| `generate_vehicle_data.py` | 生成模拟轨迹数据（1000万条） |
| `test_vehicle_tracker.py` | 查询测试和性能测试 |

## 快速开始

```python
# 行政区划查询
from pg_simple_query import find_location
result = find_location(110.995, 22.918)
# => {'province': None, 'city': '梧州市', 'district': '岑溪市', ...}

# 车辆轨迹圆形范围查询
from vehicle_tracker import find_in_circle
results = find_in_circle(116.407, 39.904, 1000)
# => [{'vehicle_id': 'V0038', 'distance_m': 452, ...}, ...]
```

## 注意事项

1. **坐标系**：数据使用 GCJ-02 坐标系（高德/腾讯地图），如输入 WGS-84（GPS）坐标需先转换
2. **查询性能**：PostgreSQL + PostGIS 方式比纯 Python 快 100+ 倍
3. **数据覆盖**：包含 34 省、392 市、3210 区县（部分边界数据可能缺失）
