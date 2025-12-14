import os
import socket
from typing import List, Optional

from jupyter_dash import JupyterDash
from dash import html, dcc, Input, Output
import plotly.express as px

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


class DashboardIncidentesSpark:
    def __init__(self, spark: SparkSession, utils, config: dict):
        self.spark = spark
        self.Utils = utils
        self.config = config

        self.df_incidentes: Optional[DataFrame] = None
        self.df_loc_xy: Optional[DataFrame] = None

        self.fecha_min = None
        self.fecha_max = None
        self.localidades: List[str] = []
        self.tipos_inc: List[str] = []


    def _resolve(self, rel_path: str) -> str:
        return self.Utils.resolve_path(rel_path, base_path=self.config.get("base_path"))

    def _path_incidentes(self) -> str:
        rel_base = self.config.get("data_procesada")
        if not rel_base:
            raise ValueError("Falta 'data_procesada' en config")
        return self._resolve(os.path.join(rel_base, "incidentes"))

    def _path_localidades_geojson(self) -> str:
        rel_comp = self.config.get("data_complementaria")
        if not rel_comp:
            raise ValueError("Falta 'data_complementaria' en config")
        return self._resolve(os.path.join(rel_comp, "localidades_xy.geojson"))

    @staticmethod
    def _safe_try_timestamp(col_expr: F.Column) -> F.Column:
        s = F.trim(col_expr.cast("string"))
        s = F.when(
            s.isNull()
            | (s == "")
            | (F.lower(s) == "nat")
            | (F.lower(s) == "nan")
            | (F.lower(s) == "none")
            | (F.lower(s) == "null"),
            F.lit(None),
        ).otherwise(s)
        return F.try_to_timestamp(s)

    @staticmethod
    def _pick_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            return int(s.getsockname()[1])

    @staticmethod
    def _fig_msg(title: str, subtitle: str = ""):
        t = title if not subtitle else f"{title}<br><sup>{subtitle}</sup>"
        return px.scatter(title=t)

    @staticmethod
    def _normalizar_codigo_localidad(col_expr: F.Column) -> F.Column:
        
        s = F.trim(col_expr.cast("string"))
        s = F.when(
            s.isNull()
            | (s == "")
            | (F.lower(s) == "nan")
            | (F.lower(s) == "none")
            | (F.lower(s) == "null"),
            F.lit(None),
        ).otherwise(s)
    
        s = F.regexp_replace(s, r"\.0$", "")
        return s

    def cargar_datos(self):
        inc_path = self._path_incidentes()
        df = self.spark.read.parquet(inc_path)

        if "FECHA" not in df.columns:
            raise ValueError("No existe la columna 'FECHA' en el parquet de incidentes.")
        df = df.withColumn("FECHA", F.to_date(F.col("FECHA")))

        ts_col = self.config.get("ts_col", "FECHA_INICIO_DESPLAZAMIENTO_MOVIL")
        if ts_col not in df.columns:
            raise ValueError(f"No existe la columna timestamp '{ts_col}'. Ajusta config['ts_col'].")

        ts_expr = self._safe_try_timestamp(F.col(ts_col))

        df = (
            df.withColumn("TS_SAFE", ts_expr)
              .withColumn("HORA", F.hour(F.col("TS_SAFE")))
              .withColumn("DOW", F.dayofweek(F.col("FECHA")))
              .withColumn(
                  "DIA_SEMANA",
                  F.when(F.col("DOW") == 2, F.lit("Lunes"))
                   .when(F.col("DOW") == 3, F.lit("Martes"))
                   .when(F.col("DOW") == 4, F.lit("Miércoles"))
                   .when(F.col("DOW") == 5, F.lit("Jueves"))
                   .when(F.col("DOW") == 6, F.lit("Viernes"))
                   .when(F.col("DOW") == 7, F.lit("Sábado"))
                   .when(F.col("DOW") == 1, F.lit("Domingo"))
                   .otherwise(F.lit(None))
              )
        )

        self.df_incidentes = df.cache()
        _ = self.df_incidentes.count()

        geojson_path = self._path_localidades_geojson()
        gjson = self.spark.read.json(geojson_path)

        loc = (
            gjson.select(F.explode("features").alias("f"))
                 .select(F.col("f.properties.*"), F.col("f.geometry.coordinates").alias("coordinates"))
                 .withColumn("Codigo_Localidad", F.col("Codigo_Localidad").cast("string"))
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
                 .select(
                     F.col("Codigo_Localidad").alias("CODIGO_LOCALIDAD_GEO"),
                     F.col("X").alias("X_GEO"),
                     F.col("Y").alias("Y_GEO")
                 )
        )

        self.df_loc_xy = loc.cache()
        _ = self.df_loc_xy.count()

        mm = self.df_incidentes.agg(F.min("FECHA").alias("min"), F.max("FECHA").alias("max")).collect()[0]
        self.fecha_min = mm["min"]
        self.fecha_max = mm["max"]

        self.localidades = [
            r["LOCALIDAD"]
            for r in (
                self.df_incidentes.select("LOCALIDAD")
                .where(F.col("LOCALIDAD").isNotNull())
                .distinct()
                .orderBy("LOCALIDAD")
                .collect()
            )
        ] if "LOCALIDAD" in self.df_incidentes.columns else []

        self.tipos_inc = [
            r["TIPO_INCIDENTE"]
            for r in (
                self.df_incidentes.select("TIPO_INCIDENTE")
                .where(F.col("TIPO_INCIDENTE").isNotNull())
                .distinct()
                .orderBy("TIPO_INCIDENTE")
                .collect()
            )
        ] if "TIPO_INCIDENTE" in self.df_incidentes.columns else []

    def crear_app(self) -> JupyterDash:
        if self.df_incidentes is None or self.df_loc_xy is None:
            self.cargar_datos()

        orden_dias = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
        app = JupyterDash(__name__)

        app.layout = html.Div(
            style={"font-family": "Arial", "margin": "20px"},
            children=[
                html.H1("Dashboard de Incidentes - Línea 123", style={"textAlign": "center"}),

                html.Div(
                    style={"display": "flex", "gap": "30px", "marginBottom": "20px", "flexWrap": "wrap"},
                    children=[
                        html.Div([
                            html.Label("Rango de fechas"),
                            dcc.DatePickerRange(
                                id="filtro-fechas",
                                start_date=self.fecha_min,
                                end_date=self.fecha_max,
                                display_format="YYYY-MM-DD",
                            )
                        ]),
                        html.Div([
                            html.Label("Localidad"),
                            dcc.Dropdown(
                                id="filtro-localidad",
                                options=[{"label": "Todas", "value": "TODAS"}] +
                                        [{"label": l, "value": l} for l in self.localidades],
                                value="TODAS",
                                clearable=False,
                            )
                        ], style={"minWidth": "250px"}),
                        html.Div([
                            html.Label("Tipos de incidente"),
                            dcc.Dropdown(
                                id="filtro-tipo",
                                options=[{"label": t, "value": t} for t in self.tipos_inc],
                                value=self.tipos_inc,
                                multi=True,
                            )
                        ], style={"minWidth": "350px"}),
                    ],
                ),

                html.Div(style={"display": "flex", "gap": "20px"}, children=[
                    html.Div([dcc.Graph(id="graf-linea")], style={"flex": 1}),
                    html.Div([dcc.Graph(id="graf-heatmap")], style={"flex": 1}),
                ]),

                html.Div(style={"display": "flex", "gap": "20px", "marginTop": "20px"}, children=[
                    html.Div([dcc.Graph(id="graf-top-localidades")], style={"flex": 1}),
                    html.Div([dcc.Graph(id="graf-top-tipos")], style={"flex": 1}),
                ]),

                html.Div(style={"display": "flex", "gap": "20px", "marginTop": "20px"}, children=[
                    html.Div([dcc.Graph(id="graf-sunburst")], style={"flex": 1}),
                    html.Div([dcc.Graph(id="graf-mapa")], style={"flex": 1}),
                ]),
            ],
        )

        @app.callback(
            Output("graf-linea", "figure"),
            Output("graf-heatmap", "figure"),
            Output("graf-top-localidades", "figure"),
            Output("graf-top-tipos", "figure"),
            Output("graf-sunburst", "figure"),
            Output("graf-mapa", "figure"),
            Input("filtro-fechas", "start_date"),
            Input("filtro-fechas", "end_date"),
            Input("filtro-localidad", "value"),
            Input("filtro-tipo", "value"),
        )
        def actualizar_dashboard(start_date, end_date, localidad_sel, tipos_sel):
            df = self.df_incidentes

            if not start_date:
                start_date = self.fecha_min
            if not end_date:
                end_date = self.fecha_max

            df = df.where((F.col("FECHA") >= F.lit(start_date)) & (F.col("FECHA") <= F.lit(end_date)))

            if localidad_sel and localidad_sel != "TODAS":
                df = df.where(F.col("LOCALIDAD") == F.lit(localidad_sel))

            if tipos_sel and isinstance(tipos_sel, list) and len(tipos_sel) > 0:
                df = df.where(F.col("TIPO_INCIDENTE").isin(tipos_sel))

            if df.limit(1).count() == 0:
                fig = self._fig_msg("Sin datos", "Los filtros dejan el dataset vacío")
                return fig, fig, fig, fig, fig, fig

            pdf_line = df.groupBy("FECHA").agg(F.count("*").alias("TOTAL")).orderBy("FECHA").toPandas()
            fig_line = px.line(pdf_line, x="FECHA", y="TOTAL", title="Tendencia diaria") if not pdf_line.empty else self._fig_msg("Tendencia", "Sin filas")

            pdf_hm = (
                df.where(F.col("HORA").isNotNull() & F.col("DIA_SEMANA").isNotNull())
                  .groupBy("DIA_SEMANA", "HORA").agg(F.count("*").alias("TOTAL"))
                  .toPandas()
            )
            fig_hm = px.density_heatmap(pdf_hm, x="HORA", y="DIA_SEMANA", z="TOTAL", title="Heatmap") if not pdf_hm.empty else self._fig_msg("Heatmap", "Sin filas")
            fig_hm.update_yaxes(categoryorder="array", categoryarray=orden_dias)

            pdf_loc = df.groupBy("LOCALIDAD").agg(F.count("*").alias("TOTAL")).orderBy(F.desc("TOTAL")).limit(10).toPandas()
            fig_loc = px.bar(pdf_loc, x="TOTAL", y="LOCALIDAD", orientation="h", title="Top 10 localidades") if not pdf_loc.empty else self._fig_msg("Top localidades", "Sin filas")

            pdf_tipo = df.groupBy("TIPO_INCIDENTE").agg(F.count("*").alias("TOTAL")).orderBy(F.desc("TOTAL")).limit(10).toPandas()
            fig_tipo = px.bar(pdf_tipo, x="TOTAL", y="TIPO_INCIDENTE", orientation="h", title="Top 10 tipos") if not pdf_tipo.empty else self._fig_msg("Top tipos", "Sin filas")

            pdf_sb = (
                df.where(F.col("PRIORIDAD_FINAL").isNotNull() & F.col("TIPO_INCIDENTE").isNotNull())
                  .groupBy("PRIORIDAD_FINAL", "TIPO_INCIDENTE").agg(F.count("*").alias("TOTAL"))
                  .toPandas()
            )
            fig_sb = px.sunburst(pdf_sb, path=["PRIORIDAD_FINAL", "TIPO_INCIDENTE"], values="TOTAL", title="Prioridad → Tipo") if not pdf_sb.empty else self._fig_msg("Sunburst", "Sin filas")

            df_count_loc = (
                df.withColumn("CODIGO_LOCALIDAD_JOIN", self._normalizar_codigo_localidad(F.col("CODIGO_LOCALIDAD")))
                  .where(F.col("CODIGO_LOCALIDAD_JOIN").isNotNull())
                  .groupBy("CODIGO_LOCALIDAD_JOIN")
                  .agg(
                      F.count("*").alias("TOTAL"),
                      F.first("LOCALIDAD", ignorenulls=True).alias("LOCALIDAD")
                  )
            )

            df_map = (
                df_count_loc.alias("a")
                .join(
                    broadcast(self.df_loc_xy).alias("b"),
                    F.col("a.CODIGO_LOCALIDAD_JOIN") == F.col("b.CODIGO_LOCALIDAD_GEO"),
                    "left"
                )
                .select(
                    F.col("a.CODIGO_LOCALIDAD_JOIN").alias("CODIGO_LOCALIDAD"),
                    F.col("a.TOTAL").alias("TOTAL"),
                    F.col("a.LOCALIDAD").alias("LOCALIDAD"),
                    F.col("b.X_GEO").alias("X"),
                    F.col("b.Y_GEO").alias("Y")
                )
            )

            pdf_map = df_map.toPandas()
            if pdf_map.empty or pdf_map["X"].isna().all() or pdf_map["Y"].isna().all():
                fig_map = self._fig_msg("Mapa", "Sin XY (verifica códigos de localidad)")
            else:
                fig_map = px.scatter_mapbox(
                    pdf_map,
                    lat="Y",
                    lon="X",
                    color="TOTAL",
                    size="TOTAL",
                    hover_name="LOCALIDAD",
                    mapbox_style="carto-positron",
                    zoom=9.5,
                    center={"lat": 4.65, "lon": -74.1},
                    title="Incidentes por localidad (centroide)"
                )

            return fig_line, fig_hm, fig_loc, fig_tipo, fig_sb, fig_map

        return app

    def run(self, jupyter_mode: str = "inline", height: int = 900, debug: bool = False, port: Optional[int] = None):
        app = self.crear_app()
        if port is None:
            port = self._pick_free_port()
        app.run_server(mode=jupyter_mode, height=height, debug=debug, port=int(port))
