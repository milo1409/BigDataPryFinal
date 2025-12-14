import os
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


class LocalidadesJoinSpark:
    def __init__(self, spark: SparkSession, utils, config: dict):
        self.spark = spark
        self.Utils = utils
        self.config = config

    def _ruta_complementaria(self, filename: str) -> str:

        base = self.config.get("base_path")
        comp_rel = self.config.get("data_complementaria")
        if not base or not comp_rel:
            raise ValueError("Faltan 'base_path' o 'data_complementaria' en config")

        rel_path = os.path.join(comp_rel, filename)
        return self.Utils.resolve_path(rel_path, base_path=base)

    def leer_localidades_xy(self, geojson_filename: str = "localidades_xy.geojson") -> DataFrame:

        geojson_path = self._ruta_complementaria(geojson_filename)

        gjson = self.spark.read.json(geojson_path)

        loc = (
            gjson
            .select(F.explode("features").alias("f"))
            .select(
                F.col("f.properties.*"),
                F.col("f.geometry.coordinates").alias("coordinates")
            )
        )

        loc = loc.withColumn("Codigo_Localidad", F.col("Codigo_Localidad").cast("string"))

        loc = (
            loc
            .withColumn(
                "X",
                F.when(F.col("POINT_X").isNotNull(), F.col("POINT_X").cast("double"))
                 .otherwise(F.col("coordinates").getItem(0).cast("double"))
            )
            .withColumn(
                "Y",
                F.when(F.col("POINT_Y").isNotNull(), F.col("POINT_Y").cast("double"))
                 .otherwise(F.col("coordinates").getItem(1).cast("double"))
            )
            .drop("coordinates")
        )

        loc = loc.select("Codigo_Localidad", "X", "Y")

        return loc

    def construir_df_agg_mapa(
        self,
        df_incidentes: DataFrame,
        geojson_filename: str = "localidades_xy.geojson",
        col_codigo_df: str = "CODIGO_LOCALIDAD",
        col_incidente: str = "NUMERO_INCIDENTE",
        col_localidad: str = "LOCALIDAD",
        how: str = "left"
    ) -> DataFrame:
       
        loc = self.leer_localidades_xy(geojson_filename)

        inc = df_incidentes.withColumn(col_codigo_df, F.col(col_codigo_df).cast("string"))

        joined = (
            broadcast(loc)
            .join(inc, loc["Codigo_Localidad"] == inc[col_codigo_df], how=how)
        )

        df_agg = (
            joined
            .groupBy(inc[col_codigo_df].alias("CODIGO_LOCALIDAD"))
            .agg(
                F.count(F.col(col_incidente)).alias("TOTAL"),
                F.avg(F.col("X")).alias("X"),
                F.avg(F.col("Y")).alias("Y"),
                F.first(F.col(col_localidad), ignorenulls=True).alias("LOCALIDAD"),
            )
        )

        return df_agg
