"""Phase 8.1 - Safe Road Management Core for roads.csv.

Provides validated CRUD operations for the road registry CSV.
All operations use atomic replacement to prevent corruption.
"""

import csv
import os
import tempfile
import uuid
from pathlib import Path
from typing import List, Dict, Optional, Tuple


# Expected CSV header (contract)
ROADS_CSV_HEADER = ["road_name", "lat", "lon"]


class RoadManagerError(Exception):
    """Base exception for road manager errors."""
    pass


class ValidationError(RoadManagerError):
    """Raised when road data fails validation."""
    pass


class DuplicateRoadError(RoadManagerError):
    """Raised when attempting to add a duplicate road name."""
    pass


class RoadNotFoundError(RoadManagerError):
    """Raised when road to remove is not found."""
    pass


class LastRoadError(RoadManagerError):
    """Raised when attempting to remove the last remaining road."""
    pass


class MalformedCSVError(RoadManagerError):
    """Raised when existing CSV structure is invalid."""
    pass


def _get_default_roads_path() -> Path:
    """Resolve default roads.csv path relative to project root."""
    return Path(__file__).resolve().parent.parent / "data" / "roads.csv"


def _normalize_name(name: str) -> str:
    """Normalize road name for comparison: strip whitespace only."""
    return name.strip()


def _validate_road_name(name: str) -> str:
    """Validate road_name field. Returns stripped name."""
    if not isinstance(name, str):
        raise ValidationError(f"road_name must be string, got {type(name).__name__}")
    stripped = name.strip()
    if not stripped:
        raise ValidationError("road_name cannot be empty or whitespace only")
    return stripped


def _validate_coordinate(value: float, field: str, min_val: float, max_val: float) -> float:
    """Validate a coordinate value (lat/lon)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{field} must be numeric, got {type(value).__name__}")
    # Check for NaN/Infinity
    if value != value or value == float('inf') or value == float('-inf'):
        raise ValidationError(f"{field} cannot be NaN or Infinity")
    if not (min_val <= value <= max_val):
        raise ValidationError(f"{field} must be between {min_val} and {max_val}, got {value}")
    return float(value)


def _validate_lat(lat) -> float:
    """Validate latitude."""
    return _validate_coordinate(lat, "lat", -90.0, 90.0)


def _validate_lon(lon) -> float:
    """Validate longitude."""
    return _validate_coordinate(lon, "lon", -180.0, 180.0)


def validate_road(road_name: str, lat, lon) -> Dict[str, object]:
    """Validate a single road entry. Returns normalized dict.
    
    Accepts lat/lon as strings (from CSV) or numeric types.
    """
    name = _validate_road_name(road_name)
    # Convert to float if string (CSV input)
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        raise ValidationError(f"lat/lon must be numeric, got lat={lat!r}, lon={lon!r}")
    lat_f = _validate_lat(lat_f)
    lon_f = _validate_lon(lon_f)
    return {"road_name": name, "lat": lat_f, "lon": lon_f}


def load_roads(path: Optional[Path] = None) -> List[Dict[str, object]]:
    """Load and validate all roads from CSV.
    
    Args:
        path: Optional custom path. Defaults to data/roads.csv.
    
    Returns:
        List of dicts with keys: road_name, lat, lon (validated).
    
    Raises:
        MalformedCSVError: If file missing, header mismatch, or any row invalid.
    """
    target_path = path if path is not None else _get_default_roads_path()
    
    if not target_path.exists():
        raise MalformedCSVError(f"roads.csv not found: {target_path}")
    
    roads = []
    seen_names = set()
    
    with open(target_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        # Verify header
        if reader.fieldnames != ROADS_CSV_HEADER:
            raise MalformedCSVError(
                f"Invalid header: expected {ROADS_CSV_HEADER}, got {reader.fieldnames}"
            )
        
        for row_num, row in enumerate(reader, start=2):
            # Validate required fields exist
            if not all(col in row for col in ROADS_CSV_HEADER):
                raise MalformedCSVError(f"Row {row_num}: missing required columns")
            
            name = row["road_name"]
            lat = row["lat"]
            lon = row["lon"]
            
            # Validate row
            try:
                validated = validate_road(name, lat, lon)
            except ValidationError as e:
                raise MalformedCSVError(f"Row {row_num}: {e}") from e
            
            # Check duplicate (case-insensitive, trimmed)
            norm_name = _normalize_name(validated["road_name"]).lower()
            if norm_name in seen_names:
                raise MalformedCSVError(
                    f"Row {row_num}: duplicate road name '{validated['road_name']}' "
                    "(case-insensitive)"
                )
            seen_names.add(norm_name)
            
            roads.append(validated)
    
    return roads


def _write_roads_atomic(roads: List[Dict[str, object]], target_path: Path) -> None:
    """Atomically write roads list to CSV using temp file + os.replace."""
    # Create temp file in same directory for atomic replace
    temp_name = f"roads.csv.tmp-{uuid.uuid4().hex[:8]}"
    temp_path = target_path.parent / temp_name
    
    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ROADS_CSV_HEADER)
            writer.writeheader()
            for road in roads:
                writer.writerow({
                    "road_name": road["road_name"],
                    "lat": road["lat"],
                    "lon": road["lon"],
                })
        # Atomic replace
        os.replace(temp_path, target_path)
    finally:
        # Cleanup temp file if it still exists (e.g., os.replace failed)
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def add_road(
    road_name: str,
    lat,
    lon,
    path: Optional[Path] = None
) -> List[Dict[str, object]]:
    """Add a new road to the registry.
    
    Args:
        road_name: Name of the road.
        lat: Latitude.
        lon: Longitude.
        path: Optional custom CSV path.
    
    Returns:
        Updated list of all roads (including new one).
    
    Raises:
        ValidationError: If new road data is invalid.
        DuplicateRoadError: If road name already exists (case-insensitive).
        MalformedCSVError: If existing file is malformed.
    """
    target_path = path if path is not None else _get_default_roads_path()
    
    # Load and validate existing roads
    existing = load_roads(target_path)
    
    # Validate new road
    new_road = validate_road(road_name, lat, lon)
    
    # Check duplicate (case-insensitive, trimmed)
    norm_new = _normalize_name(new_road["road_name"]).lower()
    for existing_road in existing:
        if _normalize_name(existing_road["road_name"]).lower() == norm_new:
            raise DuplicateRoadError(
                f"Road '{new_road['road_name']}' already exists "
                "(case-insensitive comparison)"
            )
    
    # Append new road (preserve existing order)
    updated = existing + [new_road]
    
    # Atomic write
    _write_roads_atomic(updated, target_path)
    
    return updated


def remove_road(
    road_name: str,
    path: Optional[Path] = None
) -> List[Dict[str, object]]:
    """Remove a road by name (case-insensitive, trimmed).
    
    Args:
        road_name: Name of road to remove.
        path: Optional custom CSV path.
    
    Returns:
        Updated list of roads (without removed one).
    
    Raises:
        ValidationError: If road_name is invalid.
        RoadNotFoundError: If road not found.
        LastRoadError: If removal would leave zero roads.
        MalformedCSVError: If existing file is malformed.
    """
    target_path = path if path is not None else _get_default_roads_path()
    
    # Load and validate existing roads
    existing = load_roads(target_path)
    
    # Validate input name
    name = _validate_road_name(road_name)
    norm_target = name.lower()
    
    # Find and remove (preserve order of remaining)
    found = False
    updated = []
    for road in existing:
        if _normalize_name(road["road_name"]).lower() == norm_target:
            found = True
            continue
        updated.append(road)
    
    if not found:
        raise RoadNotFoundError(f"Road '{name}' not found")
    
    # Last road safety
    if len(updated) == 0:
        raise LastRoadError("Cannot remove the last monitored road")
    
    # Atomic write
    _write_roads_atomic(updated, target_path)
    
    return updated


def get_road(road_name: str, path: Optional[Path] = None) -> Optional[Dict[str, object]]:
    """Get a single road by name (case-insensitive, trimmed). Returns None if not found."""
    target_path = path if path is not None else _get_default_roads_path()
    roads = load_roads(target_path)
    norm_target = _normalize_name(road_name).lower()
    for road in roads:
        if _normalize_name(road["road_name"]).lower() == norm_target:
            return road
    return None


if __name__ == "__main__":
    # Quick sanity check
    print("Road Manager Module - Phase 8.1")
    print("Functions: load_roads, validate_road, add_road, remove_road, get_road")
    print("Exceptions: RoadManagerError, ValidationError, DuplicateRoadError,")
    print("            RoadNotFoundError, LastRoadError, MalformedCSVError")