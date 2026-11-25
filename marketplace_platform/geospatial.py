"""Multi-city H3 geospatial intelligence engine (fine=9, medium=7, coarse=5),
1-ring neighboring-cell supply lookups, geodesic haversine ETA/distance, hot-zone definitions,
and self-hosted offline GeoJSON vector basemap generation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import h3

EARTH_RADIUS_KM = 6371.0088


@dataclass(frozen=True)
class HotZone:
    zone_id: str
    name: str
    zone_type: str  # airport, stadium, downtown, nightlife
    lat: float
    lon: float
    radius_deg: float
    demand_weight: float


@dataclass(frozen=True)
class CitySpec:
    city_id: str
    region_id: str
    display_name: str
    center_lat: float
    center_lon: float
    bbox: tuple[float, float, float, float]  # min_lat, min_lon, max_lat, max_lon
    hot_zones: tuple[HotZone, ...]


CITIES: dict[str, CitySpec] = {
    "nyc": CitySpec(
        city_id="nyc",
        region_id="us_east",
        display_name="New York City",
        center_lat=40.7505,
        center_lon=-73.9750,
        bbox=(40.630, -74.030, 40.825, -73.760),
        hot_zones=(
            HotZone("nyc_midtown", "Midtown Commute Core", "downtown", 40.7549, -73.9840, 0.018, 0.34),
            HotZone("nyc_jfk", "JFK Airport Terminal Hub", "airport", 40.6413, -73.7781, 0.016, 0.24),
            HotZone("nyc_msg_arena", "Midtown Arena / Concert Surge", "stadium", 40.7505, -73.9934, 0.012, 0.22),
            HotZone("nyc_soho_night", "Lower Manhattan Nightlife", "nightlife", 40.7233, -73.9985, 0.015, 0.20),
        ),
    ),
    "sf": CitySpec(
        city_id="sf",
        region_id="us_west",
        display_name="San Francisco",
        center_lat=37.7749,
        center_lon=-122.4194,
        bbox=(37.610, -122.510, 37.810, -122.365),
        hot_zones=(
            HotZone("sf_soma", "SoMa & FiDi Core", "downtown", 37.7850, -122.4010, 0.016, 0.35),
            HotZone("sf_sfo", "SFO Airport Pickup Zone", "airport", 37.6213, -122.3790, 0.014, 0.25),
            HotZone("sf_chase", "Mission Bay Arena", "stadium", 37.7680, -122.3877, 0.012, 0.22),
            HotZone("sf_mission", "Mission Nightlife Corridor", "nightlife", 37.7599, -122.4148, 0.015, 0.18),
        ),
    ),
    "london": CitySpec(
        city_id="london",
        region_id="eu_west",
        display_name="London",
        center_lat=51.5074,
        center_lon=-0.1278,
        bbox=(51.455, -0.460, 51.565, 0.025),
        hot_zones=(
            HotZone("ldn_city", "City & Canary Wharf", "downtown", 51.5136, -0.0890, 0.018, 0.34),
            HotZone("ldn_lhr", "Heathrow Airport Hub", "airport", 51.4700, -0.4543, 0.016, 0.24),
            HotZone("ldn_wembley", "Wembley Stadium Zone", "stadium", 51.5560, -0.2795, 0.014, 0.22),
            HotZone("ldn_soho", "Soho & West End Nightlife", "nightlife", 51.5137, -0.1365, 0.014, 0.20),
        ),
    ),
}


def validate_coordinates(lat: float, lon: float) -> bool:
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return False
    return not (abs(lat) < 1e-6 and abs(lon) < 1e-6)


def to_h3(lat: float, lon: float, resolution: int = 7) -> str:
    return str(h3.latlng_to_cell(lat, lon, resolution))


def h3_neighbors(cell: str, k: int = 1) -> list[str]:
    """Return 1-ring H3 neighbors excluding the center cell itself."""
    disk = list(h3.grid_disk(cell, k))
    return [str(c) for c in disk if str(c) != cell]


def h3_parent(cell: str, coarse_res: int = 5) -> str:
    return str(h3.cell_to_parent(cell, coarse_res))


def h3_center(cell: str) -> tuple[float, float]:
    lat, lon = h3.cell_to_latlng(cell)
    return float(lat), float(lon)


def h3_boundary_ring(cell: str) -> list[list[float]]:
    ring = [[float(lon), float(lat)] for lat, lon in h3.cell_to_boundary(cell)]
    if ring:
        ring.append(ring[0])
    return ring


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))


def estimate_eta_seconds(distance_km: float, traffic_factor: float = 1.0) -> float:
    """Geodesic pickup ETA estimate using urban speed baseline adjusted by traffic_factor."""
    urban_kmh = max(8.0, 28.0 / max(0.6, traffic_factor))
    return round((distance_km / urban_kmh) * 3600.0 + 45.0, 1)


def export_cities_basemap_geojson() -> dict[str, Any]:
    """Self-hosted offline GeoJSON vector basemap for MapLibre GL JS (zero external tile fetches)."""
    features: list[dict[str, Any]] = []
    for city in CITIES.values():
        min_lat, min_lon, max_lat, max_lon = city.bbox
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "feature_type": "city_boundary",
                    "city_id": city.city_id,
                    "region_id": city.region_id,
                    "name": city.display_name,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [min_lon, min_lat],
                            [max_lon, min_lat],
                            [max_lon, max_lat],
                            [min_lon, max_lat],
                            [min_lon, min_lat],
                        ]
                    ],
                },
            }
        )
        for hz in city.hot_zones:
            d = hz.radius_deg
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "feature_type": "hot_zone",
                        "zone_type": hz.zone_type,
                        "city_id": city.city_id,
                        "region_id": city.region_id,
                        "name": hz.name,
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [hz.lon - d, hz.lat - d * 0.75],
                                [hz.lon + d, hz.lat - d * 0.75],
                                [hz.lon + d, hz.lat + d * 0.75],
                                [hz.lon - d, hz.lat + d * 0.75],
                                [hz.lon - d, hz.lat - d * 0.75],
                            ]
                        ],
                    },
                }
            )
    return {"type": "FeatureCollection", "features": features}
