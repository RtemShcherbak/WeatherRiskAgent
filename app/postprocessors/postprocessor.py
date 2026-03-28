import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from rasterio.features import geometry_mask
from affine import Affine

from ..configs import Configs


# def build_admin_lookup(
#     self,
#     ds,
#     regions_gdf: gpd.GeoDataFrame,
#     region_col: str,
#     lat_name: str = "latitude",
#     lon_name: str = "longitude",
# ):
#     """
#     Build (lat, lon) -> admin region lookup using rasterization.
#     Returns pandas DataFrame.
#     """

#     # 1. Ensure CRS
#     if regions_gdf.crs is None:
#         raise ValueError("Regions GeoDataFrame has no CRS")
#     regions_gdf = regions_gdf.to_crs("EPSG:4326")

#     # 2. Grid
#     lats = ds[lat_name].values
#     lons = ds[lon_name].values

#     ny, nx = len(lats), len(lons)

#     # 3. Affine transform
#     res_lat = abs(lats[1] - lats[0])
#     res_lon = abs(lons[1] - lons[0])

#     transform = (
#         Affine.translation(lons.min() - res_lon / 2, lats.max() + res_lat / 2)
#         * Affine.scale(res_lon, -res_lat)
#     )

#     records = []

#     # 4. Rasterize each region
#     for _, row in regions_gdf.iterrows():
#         geom = row.geometry
#         region_name = row[region_col]

#         mask = geometry_mask(
#             [geom],
#             out_shape=(ny, nx),
#             transform=transform,
#             invert=True,
#             all_touched=False,
#         )

#         if not mask.any():
#             continue

#         iy, ix = np.where(mask)

#         records.append(
#             pd.DataFrame(
#                 {
#                     lat_name: lats[iy],
#                     lon_name: lons[ix],
#                     "region": region_name,
#                 }
#             )
#         )

#     lookup = pd.concat(records, ignore_index=True)
#     return lookup




    # def spatial_aggregate_xds(
    #     self,
    #     ds: xr.Dataset,
    #     country_name: str,
    #     lookup: pd.DataFrame,
    #     lat_name="latitude",
    #     lon_name="longitude",
    # ):
    #     """
    #     Aggregate xarray Dataset to admin regions using precomputed lookup.
    #     """
    #     var_list = list(ds.keys())

    #     df = (
    #         ds[var_list]
    #         .to_dataframe()
    #         .reset_index()
    #         .dropna(subset=var_list)
    #     )
    #     df = df.merge(
    #         lookup,
    #         on=[lat_name, lon_name],
    #         how="inner",
    #     )
    #     out = (
    #         df.groupby("region")[var_list]
    #         .mean()
    #         .reset_index()
    #     )
    #     out["country"] = country_name
    #     return out



class ProccessedData():
    def __init__(self, cfg: Configs):
        self.cfg = cfg


    def build_admin_lookup(
        self,
        ds,
        regions_gdf: gpd.GeoDataFrame,
        region_col: str,
        lat_name: str = "latitude",
        lon_name: str = "longitude",
    ):
        """
        Build (lat, lon) -> admin region lookup using rasterization.
        Returns pandas DataFrame.
        """

        # 1. Ensure CRS
        if regions_gdf.crs is None:
            raise ValueError("Regions GeoDataFrame has no CRS")
        regions_gdf = regions_gdf.to_crs("EPSG:4326")

        # 2. Grid
        lats = ds[lat_name].values
        lons = ds[lon_name].values

        ny, nx = len(lats), len(lons)

        # 3. Affine transform
        res_lat = abs(lats[1] - lats[0])
        res_lon = abs(lons[1] - lons[0])

        transform = (
            Affine.translation(lons.min() - res_lon / 2, lats.max() + res_lat / 2)
            * Affine.scale(res_lon, -res_lat)
        )

        records = []

        # 4. Rasterize each region
        for _, row in regions_gdf.iterrows():
            geom = row.geometry
            region_name = row[region_col]

            mask = geometry_mask(
                [geom],
                out_shape=(ny, nx),
                transform=transform,
                invert=True,
                all_touched=False,
            )

            if not mask.any():
                continue

            iy, ix = np.where(mask)

            records.append(
                pd.DataFrame(
                    {
                        lat_name: lats[iy],
                        lon_name: lons[ix],
                        "region": region_name,
                    }
                )
            )

        lookup = pd.concat(records, ignore_index=True)
        return lookup
    

    def build_lookup_for_source(
            self, 
            country_name: str,
            xds: xr.Dataset
        ):

        regions = gpd.read_file(self.cfg.regions_file)
        # for i, target_country in enumerate(self.cfg.target_countries):
        lookup_path = (
            self.cfg.regions_lookup_dir / 
            f"{self.cfg.source}_admin_lookup_{country_name.lower().replace(" ", "_")}.parquet"
        )
        if not lookup_path.exists():
            country_regions = regions[regions["admin"] == country_name]
            lookup = self.build_admin_lookup(
                ds = xds,
                region_col="name",
                regions_gdf = country_regions,
                # country_regions,
            )
            lookup.to_parquet(lookup_path)


    def spatial_aggregate_xds(
        self,
        xds: xr.Dataset,
        country_name: str,
        lat_name="latitude",
        lon_name="longitude",
    ):
        """
        Aggregate xarray Dataset to admin regions using precomputed lookup.
        """
        lookup = pd.read_parquet(
            self.cfg.regions_lookup_dir /
            f"{self.cfg.source}_admin_lookup_{country_name.lower().replace(" ", "_")}.parquet"
        )
        var_list = list(xds.keys())

        df = (
            xds[var_list]
            .to_dataframe()
            .reset_index()
            .dropna(subset=var_list)
        )
        df = df.merge(
            lookup,
            on=[lat_name, lon_name],
            how="inner",
        )
        out = (
            df.groupby("region")[var_list]
            .mean()
            .reset_index()
        )
        out["country"] = country_name
        return out