"""Excel conversion and explicit-schema Spark ingestion."""

from pathlib import Path
import logging
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

LOG = logging.getLogger(__name__)
MAPPING = {
    "numMena": "num_mena",
    "num_individu": "num_individu",
    "num_Deplacement": "num_deplacement",
    "Poids_extra": "poids_extra",
    "LieuDepart": "lieu_depart",
    "Origine_StrateDepart": "strate_depart",
    "HeureDepart": "heure_depart_hhmm",
    "LieuArrivee": "lieu_arrivee",
    "Dextination_StrateArrivee": "strate_arrivee",
    "HeureArrivee": "heure_arrivee_hhmm",
    "Duree (mn)": "duree_mn",
    "Motif": "motif",
    "NbTrajets": "nb_trajets",
    **{f"Mode{i}": f"mode_{i}" for i in range(1, 5)},
    "Cout": "cout",
    **{
        f"NbTrajets{x}": f"nb_trajets_{y}_pondere"
        for x, y in [
            ("APied", "apied"),
            ("Autre", "autre"),
            ("Moto", "moto"),
            ("Voiture", "voiture"),
            ("Taxi", "taxi"),
            ("Clando", "clando"),
            ("CarRapide", "car_rapide"),
            ("NN", "nn"),
            ("DDD", "ddd"),
            ("Tata", "tata"),
        ]
    },
    "Moyen de transport": "moyen_transport",
    "JourEnqueteIndi": "jour_enquete",
    "PositMenage": "position_menage",
    "Age": "age",
    "Sexe": "sexe",
}
INTS = {
    "num_mena",
    "num_individu",
    "num_deplacement",
    "lieu_depart",
    "heure_depart_hhmm",
    "lieu_arrivee",
    "heure_arrivee_hhmm",
    "duree_mn",
    "nb_trajets",
    "age",
}
DOUBLES = {"poids_extra", "cout"} | {v for v in MAPPING.values() if v.startswith("nb_trajets_")}


def convert_workbook(source: str, bronze: str) -> None:
    """Convert workbook sheets to Parquet once, keeping the OD pivot unchanged."""
    root = Path(bronze)
    root.mkdir(parents=True, exist_ok=True)
    src = Path(source)
    for sheet in pd.ExcelFile(src).sheet_names:
        target = root / f"{sheet}.parquet"
        if target.exists() and target.stat().st_mtime >= src.stat().st_mtime:
            continue
        frame = pd.read_excel(src, sheet_name=sheet, header=None if sheet == "matrice_OD" else 0)
        if sheet == "matrice_OD":
            frame = parse_od_pivot(frame)
        else:
            frame = frame.astype(object).where(pd.notna(frame), None)
            if sheet == "strate_commune":
                frame.columns = ["commune", "code_strate", "code_commune"]
            elif sheet == "explication":
                frame = frame.iloc[:, :2]
                frame.columns = ["nom_source", "definition"]
        frame.to_parquet(target, index=False)
        LOG.info("Converted sheet %s to %s", sheet, target)


def load_deplacement(spark: SparkSession, bronze: str) -> DataFrame:
    """Read source Parquet and apply target names with explicit casts."""
    schema = StructType(
        [
            StructField(
                MAPPING[source],
                (
                    IntegerType()
                    if MAPPING[source] in INTS
                    else DoubleType() if MAPPING[source] in DOUBLES else StringType()
                ),
                True,
            )
            for source in MAPPING
        ]
    )
    types = {field.name: field.dataType for field in schema.fields}
    source_df = spark.read.parquet(str(Path(bronze) / "deplacement.parquet"))
    return source_df.select(
        *[F.col(source).cast(types[target]).alias(target) for source, target in MAPPING.items()]
    )


def parse_od_pivot(frame: pd.DataFrame) -> pd.DataFrame:
    """Parse the Excel pivot whose destination headings are on row four."""
    headings = frame.iloc[3].tolist()
    destinations = [
        str(x).strip() for x in headings[1:] if pd.notna(x) and str(x).strip() != "Total général"
    ]
    rows = []
    for _, row in frame.iloc[4:].iterrows():
        origin = row.iloc[0]
        if pd.isna(origin) or str(origin).strip() == "Total général":
            continue
        for index, destination in enumerate(destinations, start=1):
            value = row.iloc[index]
            if pd.notna(value):
                rows.append((str(origin).strip(), destination, float(value)))
    return pd.DataFrame(rows, columns=["strate_depart", "strate_arrivee", "poids_total"])


def load_od_reference(spark: SparkSession, bronze: str) -> DataFrame:
    """Load the long-form reference pivot with a declared schema."""
    schema = StructType(
        [
            StructField("strate_depart", StringType(), False),
            StructField("strate_arrivee", StringType(), False),
            StructField("poids_total", DoubleType(), False),
        ]
    )
    return spark.read.schema(schema).parquet(str(Path(bronze) / "matrice_OD.parquet"))


def load_strate_dimension(spark: SparkSession, bronze: str) -> DataFrame:
    """Load the commune-to-stratum reference dimension."""
    return spark.read.parquet(str(Path(bronze) / "strate_commune.parquet")).select(
        F.col("commune").cast("string"),
        F.col("code_strate").cast("string"),
        F.col("code_commune").cast("int"),
    )
