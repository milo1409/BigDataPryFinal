import os
from typing import List
from dash import Dash, html, dcc, Input, Output
import plotly.express as px

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


class DashboardIncidentesSpark:
    def __init__(self, spark: SparkSession, utils, config: dict):
        self.spark = spark
        self.Utils = utils
        self.config = config

        self.df_incidentes: DataFrame | None = None
        self.df_loc_xy: DataFrame | None = None

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
        # dataset general sin partición
        return self._resolve(os.path.join(rel_base, "incidentes"))

    def _path_localidades_geojson(self) -> str:
        rel_comp = self.config.get("data_complementaria") 
        if not rel_comp:
            raise ValueError("Falta 'data_complementaria' en config")
        return self._resolve(os.path.join(rel_comp, "localidades_xy.geojson"))

    
    def cargar_datos(self):
        
        inc_path = self._path_incidentes()
        self.df_incidentes = self.spark.read.parquet(inc_path)

        self.df_incidentes = self.df_incidentes.withColumn(
            "FECHA",
            F.to_date(F.col("FECHA"))
        )

        # 2) Localidades XY desde GeoJSON (Spark-only)
        geojson_path = self._path_localidades_geojson()
        gjson = self.spark.read.json(geojson_path)

        loc = (
            gjson
            .select(F.explode("features").alias("f"))
            .select(
                F.col("f.properties.*"),
                F.col("f.geometry.coordinates").alias("coordinates")
            )
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
            .select("Codigo_Localidad", "X", "Y")
        )
        self.df_loc_xy = loc

        # 3) Valores para filtros (Spark -> colecta pequeña)
        mm = self.df_incidentes.agg(
            F.min("FECHA").alias("min"),
            F.max("FECHA").alias("max")
        ).collect()[0]
        self.fecha_min = mm["min"]
        self.fecha_max = mm["max"]

        self.localidades = [
            r["LOCALIDAD"]
            for r in (
                self.df_incidentes
                .select("LOCALIDAD")
                .where(F.col("LOCALIDAD").isNotNull())
                .distinct()
                .orderBy("LOCALIDAD")
                .collect()
            )
        ]

        self.tipos_inc = [
            r["TIPO_INCIDENTE"]
            for r in (
                self.df_incidentes
                .select("TIPO_INCIDENTE")
                .where(F.col("TIPO_INCIDENTE").isNotNull())
                .distinct()
                .orderBy("TIPO_INCIDENTE")
                .collect()
            )
        ]

    # ---------------------------
    # Dashboard
    # ---------------------------
    def crear_app(self) -> Dash:
        if self.df_incidentes is None or self.df_loc_xy is None:
            self.cargar_datos()

        app = Dash(__name__)

        app.layout = html.Div(
            style={"font-family": "Arial", "margin": "20px"},
            children=[
                html.H1("Dashboard de Incidentes - Linea 123", style={"textAlign": "center"}),

                # --------- FILTROS ----------
                html.Div(
                    style={"display": "flex", "gap": "30px", "marginBottom": "20px"},
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
                                options=[{"label": "Todas", "value": "TODAS"}]
                                + [{"label": l, "value": l} for l in self.localidades],
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
                    ]
                ),

                # fila 1
                html.Div(
                    style={"display": "flex", "gap": "20px"},
                    children=[
                        html.Div([dcc.Graph(id="graf-linea")], style={"flex": 1}),
                        html.Div([dcc.Graph(id="graf-heatmap")], style={"flex": 1}),
                    ]
                ),

                # fila 2
                html.Div(
                    style={"display": "flex", "gap": "20px", "marginTop": "20px"},
                    children=[
                        html.Div([dcc.Graph(id="graf-top-localidades")], style={"flex": 1}),
                        html.Div([dcc.Graph(id="graf-top-tipos")], style={"flex": 1}),
                    ]
                ),

                # fila 3
                html.Div(
                    style={"display": "flex", "gap": "20px", "marginTop": "20px"},
                    children=[
                        html.Div([dcc.Graph(id="graf-sunburst")], style={"flex": 1}),
                        html.Div([dcc.Graph(id="graf-mapa")], style={"flex": 1}),
                    ]
                ),
            ]
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
            # ---- Spark filters ----
            df = self.df_incidentes

            start = F.to_date(F.lit(start_date)) if start_date else F.lit(self.fecha_min)
            end = F.to_date(F.lit(end_date)) if end_date else F.lit(self.fecha_max)

            df = df.where((F.col("FECHA") >= start) & (F.col("FECHA") <= end))

            if localidad_sel and localidad_sel != "TODAS":
                df = df.where(F.col("LOCALIDAD") == F.lit(localidad_sel))

            if tipos_sel and isinstance(tipos_sel, list) and len(tipos_sel) > 0:
                df = df.where(F.col("TIPO_INCIDENTE").isin(tipos_sel))

            # Si no hay datos, devolver figs vacíos
            if df.limit(1).count() == 0:
                fig_empty = px.scatter(title="Sin datos para los filtros seleccionados")
                return fig_empty, fig_empty, fig_empty, fig_empty, fig_empty, fig_empty

            # ---- Agregados Spark (y luego -> pandas solo para plotly) ----

            # Tendencia diaria
            df_diario = (
                df.groupBy("FECHA")
                  .agg(F.count(F.lit(1)).alias("TOTAL"))
                  .orderBy("FECHA")
                  .toPandas()
            )
            fig_line = px.line(df_diario, x="FECHA", y="TOTAL",
                               title="Tendencia diaria de incidentes")
            fig_line.update_xaxes(title="Fecha")
            fig_line.update_yaxes(title="Número de incidentes")

            # Heatmap
            orden_dias = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
            df_hm = (
                df.groupBy("DIA_SEMANA", "HORA")
                  .agg(F.count(F.lit(1)).alias("TOTAL"))
                  .toPandas()
            )
            fig_hm = px.density_heatmap(
                df_hm, x="HORA", y="DIA_SEMANA", z="TOTAL",
                color_continuous_scale="Viridis",
                title="Heatmap: Incidentes por hora y día de semana"
            )
            fig_hm.update_yaxes(title="Día de la semana",
                                categoryorder="array",
                                categoryarray=orden_dias)
            fig_hm.update_xaxes(title="Hora")
            fig_hm.update_layout(coloraxis_colorbar_title="Número de incidentes")

            # Top 10 localidades
            df_loc = (
                df.groupBy("LOCALIDAD")
                  .agg(F.count(F.lit(1)).alias("TOTAL"))
                  .orderBy(F.desc("TOTAL"))
                  .limit(10)
                  .toPandas()
            )
            fig_loc = px.bar(df_loc, x="TOTAL", y="LOCALIDAD", orientation="h",
                             title="Top 10 localidades con más incidentes")
            fig_loc.update_xaxes(title="Número de incidentes")
            fig_loc.update_yaxes(title="Localidad")

            # Top 10 tipos
            df_tipo = (
                df.groupBy("TIPO_INCIDENTE")
                  .agg(F.count(F.lit(1)).alias("TOTAL"))
                  .orderBy(F.desc("TOTAL"))
                  .limit(10)
                  .toPandas()
            )
            fig_tipo = px.bar(df_tipo, x="TOTAL", y="TIPO_INCIDENTE", orientation="h",
                              title="Top 10 tipos de incidente")
            fig_tipo.update_xaxes(title="Número de incidentes")
            fig_tipo.update_yaxes(title="Tipo de incidente")

            # Sunburst
            df_sb = (
                df.select("PRIORIDAD_FINAL", "TIPO_INCIDENTE")
                  .where(F.col("PRIORIDAD_FINAL").isNotNull() & F.col("TIPO_INCIDENTE").isNotNull())
                  .groupBy("PRIORIDAD_FINAL", "TIPO_INCIDENTE")
                  .agg(F.count(F.lit(1)).alias("TOTAL"))
                  .toPandas()
            )
            fig_sb = px.sunburst(
                df_sb,
                path=["PRIORIDAD_FINAL", "TIPO_INCIDENTE"],
                values="TOTAL",
                title="Jerarquía: Prioridad → Tipo de incidente",
                maxdepth=2
            )

            # Mapa: contar por CODIGO_LOCALIDAD y unir con XY (broadcast)
            df_count_loc = (
                df.withColumn("CODIGO_LOCALIDAD", F.col("CODIGO_LOCALIDAD").cast("string"))
                  .groupBy("CODIGO_LOCALIDAD")
                  .agg(F.count(F.col("NUMERO_INCIDENTE")).alias("TOTAL"),
                       F.first("LOCALIDAD", ignorenulls=True).alias("LOCALIDAD"))
            )

            df_map = (
                df_count_loc
                .join(broadcast(self.df_loc_xy),
                      df_count_loc["CODIGO_LOCALIDAD"] == self.df_loc_xy["Codigo_Localidad"],
                      how="left")
                .select("CODIGO_LOCALIDAD", "TOTAL", "LOCALIDAD", "X", "Y")
            )

            pdf_map = df_map.toPandas()

            if pdf_map.empty or pdf_map["X"].isna().all():
                fig_map = px.scatter_mapbox(title="Sin datos para el mapa")
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

    def run(self, jupyter_mode: str = "inline", height: int = 900):
        app = self.crear_app()
        app.run(jupyter_mode=jupyter_mode, height=height)
