from typing import Dict, Tuple, List
import os
import pandas as pd
import re
import numpy as np


class DataLoader123:

    REEMPLAZOS_COMUNES = {
        "Ã¡": "á", "Ã©": "é", "Ã­": "í", "Ã³": "ó", "ÃÁ": "Á", "Ã‰": "É", "ÃÍ": "Í", "Ã“": "Ó", "Ãš": "Ú",
        "Ã±": "ñ", "Ã‘": "Ñ", "Ã¼": "ü", "Ãœ": "Ü", "Â¿": "¿", "Â¡": "¡", "Â°": "°", "Âª": "ª", "Âº": "º",
        "â€“": "–", "â€”": "—", "â€˜": "‘", "â€™": "’", "â€œ": "“", "â€": "”", "â€¢": "•", "â€¦": "…",
        "Ã·": "I", "Â§": "§", "÷": "I", "µ": "A", "•": "N", "‡": "O", "§": "ñ", "ê": "E", "?": "I",
        "¢": "N", "ù": "ñ", "¤": "n", "¥": "N", "à": "O", "Ö": "O", "": "e", "¡": "I",
        "\xa0": "A"
    }

    PATRON_REEMPLAZO = re.compile("|".join(map(re.escape, REEMPLAZOS_COMUNES.keys())))

    DUPLICATE_GROUPS = {
        "NUMERO_INCIDENTE": ["NUMERO_INCIDENTE"],
        "FECHA_INICIO_DESPLAZAMIENTO_MOVIL": [
            "FECHA_INICIO_DESPLAZAMIENTO_MOVIL",
            "FECHA_INICIO_DESPLAZAMIENTO_MOVIL.1",
            "FECHA_INICIO_DESPLAZAMIENTO_MOVIl",
            "FECHA_INICIO_DESPLAZAMIENTO_MOVIl.1",
            "FECHA_INICIO_DESPLAZAMIENTO-MOVIL",
            "FECHA_DESPACHO_518",
        ],
        "PRIORIDAD_FINAL": ["PRIORIDAD_FINAL", "PRIORIDAD FINAL", "PRIORIDAD"],
        "CODIGO_LOCALIDAD": ["CODIGO_LOCALIDAD", "CODIGO DE LOCALIDAD", "COD_LOCALIDAD"],
    }

    COLUMNAS_ESPERADAS = [
        "NUMERO_INCIDENTE",
        "FECHA_INICIO_DESPLAZAMIENTO_MOVIL",
        "CODIGO_LOCALIDAD",
        "LOCALIDAD",
        "EDAD",
        "UNIDAD",
        "GENERO",
        "TIPO_INCIDENTE",
        "PRIORIDAD_FINAL",
        "RECEPCION",
    ]

    @staticmethod
    def leer_csv_flexible(ruta_csv: str) -> pd.DataFrame | None:
        try:
            return pd.read_csv(ruta_csv, sep=";", encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            try:
                return pd.read_csv(ruta_csv, sep=";", encoding="latin-1", low_memory=False)
            except Exception as e:
                print(f"No se pudo leer el CSV {ruta_csv}: {e}")
                return None

    @staticmethod
    def reparar_texto_pandas(s):
        if pd.isna(s):
            return s
        if not isinstance(s, str):
            s = str(s)
        return DataLoader123.PATRON_REEMPLAZO.sub(
            lambda m: DataLoader123.REEMPLAZOS_COMUNES[m.group(0)], s
        )

    @staticmethod
    def reparar_dataframe_pandas(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        cols_texto = df.select_dtypes(include=["object", "string"]).columns
        for col_name in cols_texto:
            df[col_name] = df[col_name].apply(DataLoader123.reparar_texto_pandas)
        return df


    @staticmethod
    def unificar_columnas_duplicadas(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = df.replace(r"^\s*$", pd.NA, regex=True)

        for canon, variantes in DataLoader123.DUPLICATE_GROUPS.items():
            presentes = [c for c in variantes if c in df.columns]
            if not presentes:
                continue

            base = presentes[0]
            if base != canon:
                df[canon] = df[base]
            else:
                canon = base

            for other in presentes[1:]:
                mask = df[canon].isna() & df[other].notna()
                if mask.any():
                    df.loc[mask, canon] = df.loc[mask, other]

            drop_cols = [c for c in presentes if c != canon]
            if drop_cols:
                df = df.drop(columns=drop_cols)

        return df

    @staticmethod
    def procesar_todos_csv_crudos(config: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:

        folder_base = config["base_path"]
        ruta_cruda_rel = config["data_cruda"]
        ruta_cruda = os.path.join(folder_base, ruta_cruda_rel)

        archivos = [
            os.path.join(ruta_cruda, f)
            for f in os.listdir(ruta_cruda)
            if f.lower().endswith(".csv")
        ]

        dfs: List[pd.DataFrame] = []
        inconsistencias: List[pd.DataFrame] = [] 

        for ruta_csv in archivos:
            df = DataLoader123.leer_csv_flexible(ruta_csv)
            if df is None:
                continue

            df = DataLoader123.reparar_dataframe_pandas(df)
            df = DataLoader123.unificar_columnas_duplicadas(df)

            df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

            cols_presentes = [c for c in DataLoader123.COLUMNAS_ESPERADAS if c in df.columns]
            df = df[cols_presentes]

            dfs.append(df)

        df_total = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        df_inconsistencias_total = (
            pd.concat(inconsistencias, ignore_index=True) if inconsistencias else pd.DataFrame()
        )

        return df_total, df_inconsistencias_total

    @staticmethod
    def estandarizacion_columnas(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        cols_texto = df.select_dtypes(include=["object", "string"]).columns
        for col in cols_texto:
            df[col] = (
                df[col]
                .astype("string")
                .str.replace("\xa0", " ", regex=False)
                .str.strip()
                .str.upper()
            )

        df = DataLoader123.reparar_dataframe_pandas(df)

        mapeo_unidad = {
            "ANOS": "AÑOS",
            "AÑOS": "AÑOS",
            "AÇÑOS": "AÑOS",
            "SIN_DATO": np.nan,
            "NAN": np.nan,
            "CONVULSINN": "CONVULSION",
            "CONVULSIÓN": "CONVULSION",
            "INTOXICACINN": "INTOXICACION",
            "INTOXICACIÓN": "INTOXICACION",
            "ELECTROCUCINN / RESCATE": "ELECTROCUCION / RESCATE",
            "ELECTROCUCIÓN / RESCATE": "ELECTROCUCION / RESCATE",
            "ACCIDENTE DE AVIACINN": "ACCIDENTE DE AVIACION",
            "DOLOR TORAXCICO": "DOLOR TORACICO",
            "DOLOR TORÁCICO": "DOLOR TORACICO",
            "DOLOR TOR CICO": "DOLOR TORACICO",
            "PATOLOGÍA GINECOBSTÉTRICA": "PATOLOGIA GINECOBSTETRICA",
            "PATOLOGOA GINECOBST\x90TRICA": "PATOLOGIA GINECOBSTETRICA",
            "SÍNTOMAS GASTROINTESTINALES": "SINTOMAS GASTROINTESTINALES",
            "CAÍDA DE ALTURA": "CAIDA DE ALTURA",
            "CAODA DE ALTURA": "CAIDA DE ALTURA",
            "ACOMPAÑAMIENTO EVENTO": "ACOMPANAMIENTO EVENTO",
            "ACOEVE": "ACV",
            "CRITCA": "CRITICA",
            "ENGATIVAX": "ENGATIVA",
            "ENGATIVÁ": "ENGATIVA",
            "ENGATIV": "ENGATIVA",
            "SAN CRISTNBAL": "SAN CRISTOBAL",
            "FONTIBNN": "FONTIBON",
            "LOS MAXRTIRES": "LOS MARTIRES",
            "FONTIBÓN": "FONTIBON",
            "USAQUÉN": "USAQUEN",
            "CIUDAD BOLÍVAR": "CIUDAD BOLIVAR",
            "LOS MÁRTIRES": "LOS MARTIRES",
            "SAN CRISTÓBAL": "SAN CRISTOBAL",
            "ANTONIO NARIÑO": "ANTONIO NARINO",
            "LOS M RTIRES": "LOS MARTIRES",
            "M": "MASCULINO",
            "F": "FEMENINO",
            "608": "HERIDO",
            "603": "CONVULSION",
            "601": "ACV",
            "924": "ENFERMO",
            "609": "AMESUI",
            "613": "INCONSCIENTE",
            "604": "EVERES",
            "941": "TRASTMENT",
            "610": "INTOXICACION",
            "602": "CAIDA DE ALTURA",
            "918": "INTENTO SUICIDIO",
            "617": "SINTOMAS GASTROINTESTINALES",
            "SONTOMAS GASTROINTESTINALES": "SINTOMAS GASTROINTESTINALES",
            "607": "PATOLOGIA GINECOBSTETRICA",
            "906": "VIOLENCIA SEXUAL",
            "616": "SANGRADO VAGINAL",
            "605": "DOLOR TORAXICO",
            "611": "MALTRATO",
            "606": "ELECTROCUCION",
            "615": "QUEMADURAS",
            "715": "EVENTO NATURAL",
            "SIN_D": np.nan,
        }

        mapeo_propiedad_final = {
            "1": "CRITICA",
            "2": "ALTA",
            "3": "MEDIA",
            "4": "BAJA",
        }

        catalogo_localidades = {
            "USAQUEN": "1",
            "CHAPINERO": "2",
            "SANTA FE": "3",
            "SAN CRISTOBAL": "4",
            "USME": "5",
            "TUNJUELITO": "6",
            "BOSA": "7",
            "KENNEDY": "8",
            "FONTIBON": "9",
            "ENGATIVA": "10",
            "SUBA": "11",
            "BARRIOS UNIDOS": "12",
            "TEUSAQUILLO": "13",
            "LOS MARTIRES": "14",
            "ANTONIO NARINO": "15",
            "PUENTE ARANDA": "16",
            "CANDELARIA": "17",
            "RAFAEL URIBE URIBE": "18",
            "CIUDAD BOLIVAR": "19",
            "SUMAPAZ": "20",
            "SIN_D": "SIN_D",
        }

        correcciones_nombres_localidad = {
            "SAN CRISTNBAL": "SAN CRISTOBAL",
            "ENGATIVAX": "ENGATIVA",
            "FONTIBNN": "FONTIBON",
            "LOS MAXRTIRES": "LOS MARTIRES",
            "LA CANDELARIA": "CANDELARIA",
        }

        def estandarizar_codigo_localidad(valor):
            if pd.isna(valor):
                return "SIN_D"
            v = str(valor).strip().upper()
            if v in ("NAN", "SIN_D", "SIN_DATO", "", "NULL"):
                return "SIN_D"

            m = re.match(r"^(\d+)(?:\.0+)?$", v)
            if m:
                return m.group(1)

            if v in correcciones_nombres_localidad:
                v = correcciones_nombres_localidad[v]

            if v in catalogo_localidades:
                return catalogo_localidades[v]

            return "SIN_D"

        df["CODIGO_LOCALIDAD_STD"] = df["CODIGO_LOCALIDAD"].apply(estandarizar_codigo_localidad)
        df["CODIGO_LOCALIDAD_INT"] = (
            pd.to_numeric(
                df["CODIGO_LOCALIDAD_STD"].replace("SIN_D", np.nan),
                errors="coerce",
            ).astype("Int64")
        )
        df["CODIGO_LOCALIDAD"] = df["CODIGO_LOCALIDAD_INT"]
        df = df.drop(columns=["CODIGO_LOCALIDAD_STD", "CODIGO_LOCALIDAD_INT"])


        df["LOCALIDAD"] = df["LOCALIDAD"].replace(mapeo_unidad)
        df["GENERO"] = df["GENERO"].replace(mapeo_unidad)
        df["UNIDAD"] = df["UNIDAD"].replace(mapeo_unidad)
        df["TIPO_INCIDENTE"] = df["TIPO_INCIDENTE"].replace(mapeo_unidad)
        df["PRIORIDAD_FINAL"] = df["PRIORIDAD_FINAL"].replace(mapeo_unidad)
        df["PRIORIDAD_FINAL"] = df["PRIORIDAD_FINAL"].replace(mapeo_propiedad_final)

        df["EDAD"] = pd.to_numeric(df["EDAD"], errors="coerce").astype("Int64")

        df = DataLoader123.reparar_dataframe_pandas(df)

        df["FECHA_INICIO_DESPLAZAMIENTO_MOVIL"] = pd.to_datetime(
            df["FECHA_INICIO_DESPLAZAMIENTO_MOVIL"], errors="coerce"
        )
        df["FECHA"] = df["FECHA_INICIO_DESPLAZAMIENTO_MOVIL"].dt.date
        df["HORA"] = df["FECHA_INICIO_DESPLAZAMIENTO_MOVIL"].dt.hour
        df["DIA_SEMANA_EN"] = df["FECHA_INICIO_DESPLAZAMIENTO_MOVIL"].dt.day_name()

        map_dias = {
            "Monday": "Lunes",
            "Tuesday": "Martes",
            "Wednesday": "Miércoles",
            "Thursday": "Jueves",
            "Friday": "Viernes",
            "Saturday": "Sábado",
            "Sunday": "Domingo",
        }

        df["DIA_SEMANA"] = df["DIA_SEMANA_EN"].map(map_dias)

        return df
