import os
import shutil
from typing import Dict, List, Union
import pandas as pd

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from utilities import Utils


class ExtraerDatosProcesamiento:
    
    def __init__(self, spark: SparkSession, utils, config: dict):
        self.spark = spark
        self.Utils = utils
        self.config = config

    def _resolve(self, rel_path: str) -> str:
        return self.Utils.resolve_path(rel_path, base_path=self.config.get("base_path"))

    def _path_procesada(self) -> str:
        rel = self.config.get("data_procesada")
        if not rel:
            raise ValueError("Falta 'data_procesada' en config")
        return self._resolve(rel)

    def _path_dashboard(self) -> str:
        rel = self.config.get("data_dashboard")
        if not rel:
            raise ValueError("Falta 'data_dashboard' en config")
        return self._resolve(rel)

    
    def limpiar_carpetas_salida(
        self,
        limpiar_procesada: bool = True,
        limpiar_dashboard: bool = True
    ) -> Dict[str, str]:
        borradas: Dict[str, str] = {}

        if limpiar_procesada:
            p = self._path_procesada()
            if os.path.exists(p):
                shutil.rmtree(p)
            os.makedirs(p, exist_ok=True)
            borradas["procesada"] = p

        if limpiar_dashboard:
            d = self._path_dashboard()
            if os.path.exists(d):
                shutil.rmtree(d)
            os.makedirs(d, exist_ok=True)
            borradas["dashboard"] = d

        return borradas

    
    def _pandas_to_spark_force_string(self, pdf: pd.DataFrame) -> DataFrame:
        pdf2 = pdf.copy()
        pdf2 = pdf2.where(pd.notna(pdf2), None)

        for c in pdf2.columns:
            pdf2[c] = pdf2[c].apply(lambda x: None if x is None else str(x))

        schema = StructType([StructField(str(c), StringType(), True) for c in pdf2.columns])
        records = pdf2.to_dict(orient="records")
        return self.spark.createDataFrame(records, schema=schema)

    def _ensure_spark_df(self, df: Union[pd.DataFrame, DataFrame]) -> DataFrame:
        if isinstance(df, pd.DataFrame):
            return self._pandas_to_spark_force_string(df)
        if isinstance(df, DataFrame):
            return df
        raise TypeError("df debe ser un pandas.DataFrame o un pyspark.sql.DataFrame")

    
    def _try_int(self, colname: str):
        
        c = F.col(colname)
        s = F.regexp_replace(F.trim(c.cast("string")), ",", ".")

        return F.when(s.rlike(r"^\d+$"), s.cast("int")) \
                .when(s.rlike(r"^\d+\.0+$"), F.regexp_replace(s, r"\.0+$", "").cast("int")) \
                .when(s.rlike(r"^\d+(\.\d+)?$"), s.cast("double").cast("int")) \
                .otherwise(F.lit(None).cast("int"))

    
    def _write_parquet(self, df: DataFrame, abs_out_dir: str, mode: str = "overwrite") -> str:
        df.write.mode(mode).parquet(abs_out_dir)
        return abs_out_dir

    def _write_partitioned_parquet(
        self,
        df: DataFrame,
        abs_out_dir: str,
        partition_cols: List[str],
        mode: str = "overwrite",
    ) -> str:
        df.write.mode(mode).partitionBy(*partition_cols).parquet(abs_out_dir)
        return abs_out_dir

    @Utils.perf_logger(base_path=os.getenv("BASE_PATH", ""), name="generar_parquets_dashboard_spark")
    def generar_parquets_dashboard_spark(
        self,
        df: Union[pd.DataFrame, DataFrame],
        mode: str = "overwrite",
        limpiar_procesada: bool = True,
        limpiar_dashboard: bool = True,
        subdir_general: str = "incidentes",
    ) -> Dict[str, str]:

        
        self.limpiar_carpetas_salida(
            limpiar_procesada=limpiar_procesada,
            limpiar_dashboard=limpiar_dashboard,
        )

        
        df_s = self._ensure_spark_df(df)

        if "FECHA" in df_s.columns:
            df_s = df_s.withColumn("FECHA", F.to_date(F.col("FECHA")))

        if "HORA" in df_s.columns:
            df_s = df_s.withColumn("HORA", self._try_int("HORA"))

        if "EDAD" in df_s.columns:
            df_s = df_s.withColumn("EDAD", self._try_int("EDAD"))

        rutas: Dict[str, str] = {}

        
        out_general = os.path.join(self._path_procesada(), subdir_general)
        rutas["general"] = self._write_parquet(df_s, out_general, mode=mode)

        
        df_diario = df_s.groupBy("FECHA").agg(F.count(F.lit(1)).alias("TOTAL"))

        df_hm = df_s.groupBy("DIA_SEMANA", "HORA").agg(F.count(F.lit(1)).alias("TOTAL"))

        df_loc = df_s.groupBy("LOCALIDAD").agg(F.count(F.lit(1)).alias("TOTAL"))

        df_tipo = df_s.groupBy("TIPO_INCIDENTE").agg(F.count(F.lit(1)).alias("TOTAL"))

        df_sb = df_s.groupBy("PRIORIDAD_FINAL", "TIPO_INCIDENTE").agg(F.count(F.lit(1)).alias("TOTAL"))

        
        dash_root = self._path_dashboard()

        rutas["diario"] = self._write_partitioned_parquet(
            df=df_diario,
            abs_out_dir=os.path.join(dash_root, "agg_diario"),
            partition_cols=["FECHA"],
            mode=mode,
        )

        rutas["heatmap"] = self._write_partitioned_parquet(
            df=df_hm,
            abs_out_dir=os.path.join(dash_root, "agg_heatmap"),
            partition_cols=["DIA_SEMANA", "HORA"],
            mode=mode,
        )

        rutas["localidad"] = self._write_partitioned_parquet(
            df=df_loc,
            abs_out_dir=os.path.join(dash_root, "agg_localidad"),
            partition_cols=["LOCALIDAD"],
            mode=mode,
        )

        rutas["tipo_incidente"] = self._write_partitioned_parquet(
            df=df_tipo,
            abs_out_dir=os.path.join(dash_root, "agg_tipo_incidente"),
            partition_cols=["TIPO_INCIDENTE"],
            mode=mode,
        )

        rutas["sunburst"] = self._write_partitioned_parquet(
            df=df_sb,
            abs_out_dir=os.path.join(dash_root, "agg_prioridad_tipo"),
            partition_cols=["PRIORIDAD_FINAL", "TIPO_INCIDENTE"],
            mode=mode,
        )

        return rutas
