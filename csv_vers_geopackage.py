"""
CSV → GeoPackage
================
Convertit les CSV déjà produits par sirene_fpe_extraction.py
en fichiers GeoPackage (.gpkg) pour import sur cartes.gouv.fr.

Ne nécessite pas les fichiers SIRENE ni de géocodage.
Lit uniquement les CSV du dossier output_sirene/.

Usage :
    python csv_vers_geopackage.py
"""

import os
import glob
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Dossier contenant les CSV déjà produits
DOSSIER_OUTPUT = "output_sirene"

# Fichiers à ignorer (pas des couches de données)
FICHIERS_EXCLUS = {"_recapitulatif.csv", "adresses_a_geocoder.csv"}

# =============================================================================

def csv_vers_gpkg(chemin_csv):
    nom_fichier = os.path.basename(chemin_csv)
    nom_base    = nom_fichier.replace(".csv", "")
    chemin_gpkg = chemin_csv.replace(".csv", ".gpkg")

    print(f"  Lecture : {nom_fichier}", end=" ... ", flush=True)
    try:
        df = pd.read_csv(chemin_csv, dtype=str, encoding="utf-8-sig", on_bad_lines="skip")
    except Exception as e:
        print(f"ERREUR lecture : {e}")
        return

    if "latitude" not in df.columns or "longitude" not in df.columns:
        print("ignoré (pas de colonnes latitude/longitude)")
        return

    df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    df_geo = df.dropna(subset=["latitude", "longitude"])
    nb_total  = len(df)
    nb_geo    = len(df_geo)

    if nb_geo == 0:
        print(f"ignoré (0 point géocodé sur {nb_total})")
        return

    geometry = [Point(lon, lat) for lon, lat in zip(df_geo["longitude"], df_geo["latitude"])]
    gdf = gpd.GeoDataFrame(df_geo, geometry=geometry, crs="EPSG:4326")
    gdf.to_file(chemin_gpkg, driver="GPKG", layer=nom_base)

    print(f"OK  {nb_geo:,} points  ({nb_total - nb_geo:,} sans coordonnees ignores) -> {os.path.basename(chemin_gpkg)}")


def main():
    print()
    print("=" * 50)
    print("  CSV -> GeoPackage (cartes.gouv / QGIS)")
    print("=" * 50)
    print()

    fichiers = sorted(glob.glob(os.path.join(DOSSIER_OUTPUT, "*.csv")))
    fichiers = [f for f in fichiers if os.path.basename(f) not in FICHIERS_EXCLUS]

    if not fichiers:
        print(f"Aucun CSV trouvé dans : {DOSSIER_OUTPUT}/")
        print("Lance d'abord sirene_fpe_extraction.py pour produire les CSV.")
        return

    print(f"{len(fichiers)} fichier(s) CSV trouvé(s) dans {DOSSIER_OUTPUT}/\n")

    for chemin in fichiers:
        csv_vers_gpkg(chemin)

    print()
    print("Termine ! Les .gpkg sont dans :", DOSSIER_OUTPUT)
    print()


if __name__ == "__main__":
    main()
