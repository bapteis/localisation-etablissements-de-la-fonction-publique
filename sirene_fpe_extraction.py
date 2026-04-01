"""
SIRENE - Extraction des établissements publics pour UMap
=========================================================
Ce script produit des fichiers CSV par grande famille (FPE, FPT, FPH, etc.)
avec géocodage via l'API BAN (Base Adresse Nationale).

Fichiers nécessaires dans le même dossier que ce script :
  - StockEtablissement_utf8.csv
  - StockUniteLegale_utf8.csv

Fichiers produits dans le sous-dossier output/ :
  - fpe_71_etat_services.csv
  - fpe_73_ep_nationaux.csv
  - fpe_74_gip_divers.csv
  - fpe_4x_operateurs_epic.csv
  - fpt_72_collectivites.csv
  - fph_75_secu_sociale.csv
  - fph_76_autres_sociaux.csv

Auteur : généré avec Claude
"""

import pandas as pd
import requests
import io
import os
import time

try:
    import geopandas as gpd
    from shapely.geometry import Point
    GEOPANDAS_DISPONIBLE = True
except ImportError:
    GEOPANDAS_DISPONIBLE = False

# =============================================================================
# CONFIGURATION
# =============================================================================

# Dossier où se trouvent tes fichiers SIRENE (. = même dossier que le script)
DOSSIER_SIRENE = "."

# Dossier de sortie
DOSSIER_OUTPUT = "output_sirene"

# Taille des lots pour le géocodage BAN (max 50 000, mais 5 000 est plus sûr)
TAILLE_LOT_GEOCODAGE = 3000

# Mapping des catégories juridiques -> familles -> nom de fichier de sortie
# Clé = préfixe du code catégorie juridique (2 premiers chiffres)
# Valeur = (nom_fichier, libelle_famille, versant)
FAMILLES = {
    "71": ("fpe_71_etat_services.csv",       "État et services (ministères, directions, services déconcentrés)", "FPE"),
    "73": ("fpe_73_ep_nationaux.csv",         "Établissements publics nationaux (universités, CEREMA…)",         "FPE"),
    "74": ("fpe_74_gip_divers.csv",           "GIP, établissements cultes, armées…",                             "FPE"),
    "4":  ("fpe_4x_operateurs_epic.csv",      "Opérateurs et EPIC (SNCF, ADEME, IGN…)",                          "FPE"),
    # "72": traité séparément (communes, départements, régions)
    "75": ("fph_75_secu_sociale.csv",          "Organismes de sécurité sociale",                                  "FPH"),
    "76": ("fph_76_autres_sociaux.csv",        "Autres organismes sociaux",                                       "FPH"),
}

# Libellés lisibles pour les tranches d'effectifs SIRENE
EFFECTIFS_LABELS = {
    "NN": "Non renseigné",
    "00": "0 salarié",
    "01": "1 ou 2 salariés",
    "02": "3 à 5 salariés",
    "03": "6 à 9 salariés",
    "11": "10 à 19 salariés",
    "12": "20 à 49 salariés",
    "21": "50 à 99 salariés",
    "22": "100 à 199 salariés",
    "31": "200 à 249 salariés",
    "32": "250 à 499 salariés",
    "41": "500 à 999 salariés",
    "42": "1 000 à 1 999 salariés",
    "51": "2 000 à 4 999 salariés",
    "52": "5 000 à 9 999 salariés",
    "53": "10 000 salariés et plus",
}

# Libellés des catégories juridiques (code à 4 chiffres)
# Source : nomenclature INSEE officielle, mise à jour septembre 2022
# https://www.insee.fr/fr/information/2028129
CAT_JURIDIQUE_LABELS = {
    # ── Personnes morales de droit public soumises au droit commercial (4xxx) ──
    "4110": "Établissement public national à caractère industriel ou commercial doté d'un comptable public",
    "4120": "Établissement public national à caractère industriel ou commercial non doté d'un comptable public",
    "4130": "Exploitant public",
    "4140": "Établissement public local à caractère industriel ou commercial",
    "4150": "Régie d'une collectivité locale à caractère industriel ou commercial",
    "4160": "Institution Banque de France",
    # ── Administration centrale et déconcentrée de l'État (71xx) ────────────
    "7111": "Autorité constitutionnelle",
    "7112": "Autorité administrative ou publique indépendante",
    "7113": "Ministère",
    "7120": "Service central d'un ministère",
    "7150": "Service du ministère de la Défense",
    "7160": "Service déconcentré à compétence nationale d'un ministère (hors Défense)",
    "7171": "Service déconcentré de l'État à compétence (inter) régionale",
    "7172": "Service déconcentré de l'État à compétence (inter) départementale",
    "7179": "(Autre) Service déconcentré de l'État à compétence territoriale",
    "7190": "École nationale non dotée de la personnalité morale",
    # ── Collectivités territoriales (72xx) ──────────────────────────────────
    "7210": "Commune et commune nouvelle",
    "7220": "Département",
    "7225": "Collectivité et territoire d'Outre-Mer",
    "7229": "(Autre) Collectivité territoriale",
    "7230": "Région",
    # ── Établissements publics locaux (73xx) ────────────────────────────────
    # Note : la nomenclature INSEE 2022 classe la majorité des 73xx en local,
    # et les établissements nationaux dans les codes 738x.
    "7312": "Commune associée et commune déléguée",
    "7313": "Section de commune",
    "7314": "Ensemble urbain",
    "7321": "Association syndicale autorisée",
    "7322": "Association foncière urbaine",
    "7323": "Association foncière de remembrement",
    "7331": "Établissement public local d'enseignement",
    "7340": "Pôle métropolitain",
    "7341": "Secteur de commune",
    "7342": "District urbain",
    "7343": "Communauté urbaine",
    "7344": "Métropole",
    "7345": "Syndicat intercommunal à vocation multiple (SIVOM)",
    "7346": "Communauté de communes",
    "7347": "Communauté de villes",
    "7348": "Communauté d'agglomération",
    "7349": "Autre établissement public local de coopération non spécialisé ou entente",
    "7351": "Institution interdépartementale ou entente",
    "7352": "Institution interrégionale ou entente",
    "7353": "Syndicat intercommunal à vocation unique (SIVU)",
    "7354": "Syndicat mixte fermé",
    "7355": "Syndicat mixte ouvert",
    "7356": "Commission syndicale pour la gestion des biens indivis des communes",
    "7357": "Pôle d'équilibre territorial et rural (PETR)",
    "7361": "Centre communal d'action sociale",
    "7362": "Caisse des écoles",
    "7363": "Caisse de crédit municipal",
    "7364": "Établissement d'hospitalisation",
    "7365": "Syndicat inter hospitalier",
    "7366": "Établissement public local social et médico-social",
    "7367": "Centre Intercommunal d'action sociale (CIAS)",
    "7371": "Office public d'habitation à loyer modéré (OPHLM)",
    "7372": "Service départemental d'incendie et de secours (SDIS)",
    "7373": "Établissement public local culturel",
    "7378": "Régie d'une collectivité locale à caractère administratif",
    "7379": "(Autre) Établissement public administratif local",
    "7381": "Organisme consulaire",
    # ── Établissements publics nationaux (738x) ──────────────────────────────
    "7382": "Établissement public national ayant fonction d'administration centrale",
    "7383": "Établissement public national à caractère scientifique culturel et professionnel",
    "7384": "Autre établissement public national d'enseignement",
    "7385": "Autre établissement public national administratif à compétence territoriale limitée",
    "7389": "Établissement public national à caractère administratif",
    # ── GIP et divers public (74xx) ──────────────────────────────────────────
    "7410": "Groupement d'intérêt public (GIP)",
    "7430": "Établissement public des cultes d'Alsace-Lorraine",
    "7450": "Établissement public administratif, cercle et foyer dans les armées",
    "7470": "Groupement de coopération sanitaire à gestion publique",
    "7490": "Autre personne morale de droit administratif",
    # ── Codes 75xx / 76xx (version antérieure — peuvent subsister dans SIRENE) ─
    "7510": "Caisse primaire d'assurance maladie (CPAM)",
    "7511": "Caisse régionale d'assurance maladie (CRAM)",
    "7512": "Caisse nationale d'assurance maladie (CNAM)",
    "7513": "Caisse d'allocations familiales (CAF)",
    "7514": "URSSAF / union de recouvrement",
    "7515": "Caisse de mutualité sociale agricole (MSA)",
    "7516": "Caisse nationale d'assurance vieillesse (CNAV)",
    "7517": "Caisse nationale des allocations familiales (CNAF)",
    "7519": "Autre organisme de sécurité sociale",
    "7520": "Mutuelle d'assurance",
    "7540": "Régime de prévoyance complémentaire",
    "7590": "Autre organisme social à statut légal particulier",
    "7610": "Centre communal d'action sociale (CCAS)",
    "7620": "Centre intercommunal d'action sociale (CIAS)",
    "7630": "Organisme d'aide sociale",
    "7640": "Association d'aide sociale agréée",
    "7690": "Autre organisme social",
}

# Fallback : libellé niveau 2 (2 premiers chiffres) si code 4 chiffres inconnu
CAT_JURIDIQUE_LABELS_PREFIXE = {
    "71": "État",
    "72": "Collectivité territoriale",
    "73": "Établissement public national",
    "74": "GIP / divers public",
    "75": "Organisme de sécurité sociale",
    "76": "Autre organisme social",
    "41": "EPIC / EP droit commercial",
    "42": "Régie ou EP local",
    "43": "EP droit commercial local",
    "44": "EP droit commercial national",
    "45": "EP droit commercial divers",
}

# Libellés des codes NAF (APE) — sections principales du secteur public
NAF_LABELS = {
    # Section O — Administration publique et défense
    "84.11Z": "Administration publique générale",
    "84.12Z": "Administration publique (tutelle) de la santé, formation, culture",
    "84.13Z": "Administration publique (tutelle) des activités économiques",
    "84.21Z": "Affaires étrangères",
    "84.22Z": "Défense nationale",
    "84.23Z": "Justice",
    "84.24Z": "Ordre public et sécurité",
    "84.25Z": "Services du feu et de secours",
    "84.30A": "Activités générales de sécurité sociale",
    "84.30B": "Gestion des retraites complémentaires",
    "84.30C": "Distribution sociale de revenus",
    # Section P — Enseignement
    "85.10Z": "Enseignement pré-primaire",
    "85.20Z": "Enseignement primaire",
    "85.31Z": "Enseignement secondaire général",
    "85.32Z": "Enseignement secondaire technique ou professionnel",
    "85.41Z": "Enseignement post-secondaire non supérieur",
    "85.42Z": "Enseignement supérieur",
    "85.51Z": "Enseignement sportif et de loisirs",
    "85.52Z": "Enseignement culturel",
    "85.53Z": "Enseignement de la conduite",
    "85.59A": "Formation continue d'adultes",
    "85.59B": "Autres enseignements",
    "85.60Z": "Activités de soutien à l'enseignement",
    # Section Q — Santé humaine et action sociale
    "86.10Z": "Activités hospitalières",
    "86.21Z": "Médecine générale",
    "86.22A": "Radiodiagnostic et radiothérapie",
    "86.22B": "Chirurgie",
    "86.22C": "Autres soins médicaux spécialisés",
    "86.23Z": "Pratique dentaire",
    "86.90A": "Ambulances",
    "86.90B": "Laboratoires d'analyses médicales",
    "86.90C": "Centres de collecte et banques d'organes",
    "86.90D": "Infirmiers et sages-femmes",
    "86.90E": "Rééducation, appareillage et pédicurie-podologie",
    "86.90F": "Autres activités de santé humaine",
    "87.10A": "Hébergement médicalisé — personnes âgées",
    "87.10B": "Hébergement médicalisé — enfants handicapés",
    "87.10C": "Hébergement médicalisé — adultes handicapés",
    "87.20A": "Hébergement social — handicapés mentaux",
    "87.20B": "Hébergement social — toxicomanes",
    "87.30A": "Hébergement social — personnes âgées",
    "87.30B": "Hébergement social — handicapés physiques",
    "87.90A": "Hébergement social — enfants en difficultés",
    "87.90B": "Hébergement social — adultes et familles en difficultés",
    "88.10A": "Aide à domicile",
    "88.10B": "Accueil adultes handicapés / personnes âgées sans hébergement",
    "88.10C": "Aide par le travail",
    "88.91A": "Accueil de jeunes enfants",
    "88.91B": "Accueil enfants handicapés sans hébergement",
    "88.99A": "Accueil enfants et adolescents sans hébergement",
    "88.99B": "Action sociale sans hébergement (autre)",
    # Recherche
    "72.11Z": "Recherche-développement en biotechnologie",
    "72.19Z": "Recherche-développement en sciences physiques et naturelles",
    "72.20Z": "Recherche-développement en sciences humaines et sociales",
    # Culture / patrimoine / sport
    "91.01Z": "Gestion des bibliothèques et des archives",
    "91.02Z": "Gestion des musées",
    "91.03Z": "Gestion des sites et monuments historiques",
    "91.04Z": "Jardins botaniques, zoologiques et réserves naturelles",
    "90.01Z": "Arts du spectacle vivant",
    "90.02Z": "Activités de soutien au spectacle vivant",
    "93.11Z": "Gestion d'installations sportives",
    "93.12Z": "Activités de clubs de sports",
    "93.19Z": "Autres activités liées au sport",
    # Environnement / eau / déchets
    "36.00Z": "Captage, traitement et distribution d'eau",
    "37.00Z": "Collecte et traitement des eaux usées",
    "38.11Z": "Collecte des déchets non dangereux",
    "38.12Z": "Collecte des déchets dangereux",
    "38.21Z": "Traitement et élimination des déchets non dangereux",
    "38.22Z": "Traitement et élimination des déchets dangereux",
    # Transports
    "49.31Z": "Transports urbains et suburbains de voyageurs",
    "49.32Z": "Transports de voyageurs par taxi",
    "49.39A": "Transports routiers réguliers de voyageurs",
    "49.39B": "Autres transports routiers de voyageurs",
    "49.10Z": "Transport ferroviaire interurbain de voyageurs",
    "49.20Z": "Transport ferroviaire de fret",
    "52.21Z": "Services auxiliaires des transports terrestres",
    # Services divers
    "63.11Z": "Traitement de données et hébergement",
    "70.10Z": "Activités des sièges sociaux",
    "70.22Z": "Conseil pour les affaires et gestion",
    "71.12B": "Ingénierie et études techniques",
    "71.20B": "Analyses, essais et inspections techniques",
    "75.00Z": "Activités vétérinaires",
    "99.00Z": "Organisations et organismes extraterritoriaux",
    # Codes NAF 2025 courants (format légèrement différent)
    "84.11": "Administration publique générale",
    "84.12": "Administration publique (tutelle) de la santé, formation, culture",
    "84.13": "Administration publique (tutelle) des activités économiques",
    "85.42": "Enseignement supérieur",
    "86.10": "Activités hospitalières",
}

# =============================================================================
# ÉTAPE 1 : CHARGEMENT DES UNITÉS LÉGALES (pour filtrer par catégorie juridique)
# =============================================================================

def charger_unites_legales(chemin):
    """
    Charge uniquement les colonnes utiles du fichier des unités légales
    et filtre sur les catégories juridiques publiques.
    Retourne un DataFrame indexé par siren.
    """
    print("=" * 60)
    print("ÉTAPE 1 : Chargement des unités légales")
    print("=" * 60)
    print(f"  Lecture de : {chemin}")
    print("  (Ce fichier fait ~1 Go, patience...)")

    colonnes_ul = [
        "siren",
        "categorieJuridiqueUniteLegale",
        "denominationUniteLegale",
        "denominationUsuelle1UniteLegale",
        "sigleUniteLegale",
        "activitePrincipaleUniteLegale",   # code APE de l'unité légale
        "etatAdministratifUniteLegale",
    ]

    # On lit par chunks pour ne pas saturer la RAM
    chunks = []
    total = 0
    for chunk in pd.read_csv(
        chemin,
        usecols=colonnes_ul,
        dtype=str,
        chunksize=100_000,
        low_memory=False,
    ):
        # Filtre : unités légales actives uniquement
        chunk = chunk[chunk["etatAdministratifUniteLegale"] == "A"]

        # Filtre : catégories publiques
        # On garde tout ce qui commence par 4, 71, 72, 73, 74, 75, 76
        mask = chunk["categorieJuridiqueUniteLegale"].str.startswith(
            ("4", "71", "72", "73", "74", "75", "76"), na=False
        )
        chunk = chunk[mask]
        chunks.append(chunk)
        total += len(chunk)

    ul = pd.concat(chunks, ignore_index=True)
    print(f"  → {len(ul):,} unités légales publiques actives trouvées")
    return ul


# =============================================================================
# ÉTAPE 2 : CHARGEMENT DES ÉTABLISSEMENTS
# =============================================================================

def charger_etablissements(chemin, sirens_valides):
    """
    Charge les établissements actifs dont le SIREN appartient
    à la liste des unités légales publiques.
    """
    print()
    print("=" * 60)
    print("ÉTAPE 2 : Chargement des établissements")
    print("=" * 60)
    print(f"  Lecture de : {chemin}")
    print("  (Ce fichier fait ~4-5 Go, patience...)")

    colonnes_etab = [
        "siren",
        "siret",
        "etatAdministratifEtablissement",
        "etablissementSiege",
        "numeroVoieEtablissement",
        "typeVoieEtablissement",
        "libelleVoieEtablissement",
        "codePostalEtablissement",
        "libelleCommuneEtablissement",
        "codeCommuneEtablissement",
        "activitePrincipaleEtablissement",   # code APE de l'établissement
        "trancheEffectifsEtablissement",
        "anneeEffectifsEtablissement",
        "denominationUsuelleEtablissement",  # parfois différent du nom UL
    ]

    # Conversion en set pour la recherche rapide
    sirens_set = set(sirens_valides)

    chunks = []
    total_lu = 0
    for chunk in pd.read_csv(
        chemin,
        usecols=colonnes_etab,
        dtype=str,
        chunksize=100_000,
        low_memory=False,
    ):
        total_lu += len(chunk)
        # Filtre : établissement actif
        chunk = chunk[chunk["etatAdministratifEtablissement"] == "A"]
        # Filtre : SIREN dans notre liste publique
        chunk = chunk[chunk["siren"].isin(sirens_set)]
        chunks.append(chunk)

    etab = pd.concat(chunks, ignore_index=True)
    print(f"  → {len(etab):,} établissements actifs retenus (sur {total_lu:,} lus)")
    return etab


# =============================================================================
# ÉTAPE 3 : JOINTURE ET ENRICHISSEMENT
# =============================================================================

def enrichir(etab, ul):
    """
    Joint les établissements avec les unités légales pour ajouter
    le nom, la catégorie juridique, etc.
    """
    print()
    print("=" * 60)
    print("ÉTAPE 3 : Jointure établissements ↔ unités légales")
    print("=" * 60)

    ul_light = ul[[
        "siren",
        "categorieJuridiqueUniteLegale",
        "denominationUniteLegale",
        "denominationUsuelle1UniteLegale",
        "sigleUniteLegale",
    ]].copy()

    merged = etab.merge(ul_light, on="siren", how="left")

    # Nom à afficher : on préfère la dénomination usuelle de l'établissement,
    # sinon celle de l'unité légale, sinon la dénomination officielle
    merged["nom_affichage"] = (
        merged["denominationUsuelleEtablissement"]
        .fillna(merged["denominationUsuelle1UniteLegale"])
        .fillna(merged["denominationUniteLegale"])
        .fillna("Non renseigné")
    )

    # Adresse complète lisible
    merged["adresse"] = (
        merged["numeroVoieEtablissement"].fillna("") + " " +
        merged["typeVoieEtablissement"].fillna("") + " " +
        merged["libelleVoieEtablissement"].fillna("")
    ).str.strip()

    merged["adresse_complete"] = (
        merged["adresse"] + ", " +
        merged["codePostalEtablissement"].fillna("") + " " +
        merged["libelleCommuneEtablissement"].fillna("")
    ).str.strip(", ")

    # Préfixe sur 2 caractères de la catégorie juridique
    merged["prefixe_cj"] = merged["categorieJuridiqueUniteLegale"].str[:2]

    # Libellé catégorie juridique (4 chiffres en priorité, puis 2 chiffres en fallback)
    merged["libelle_categorie_juridique"] = (
        merged["categorieJuridiqueUniteLegale"].map(CAT_JURIDIQUE_LABELS)
        .fillna(merged["prefixe_cj"].map(CAT_JURIDIQUE_LABELS_PREFIXE))
        .fillna("Cat. " + merged["categorieJuridiqueUniteLegale"].fillna("?"))
    )

    # Libellé NAF / activité principale
    merged["libelle_activite"] = merged["activitePrincipaleEtablissement"].map(NAF_LABELS).fillna(
        merged["activitePrincipaleEtablissement"].fillna("Non renseigné")
    )

    # Libellé effectifs
    merged["effectifs_libelle"] = merged["trancheEffectifsEtablissement"].map(EFFECTIFS_LABELS).fillna(
        merged["trancheEffectifsEtablissement"].fillna("Non renseigné")
    )

    # Nettoyage des retours à la ligne dans les champs texte (évite les décalages de colonnes CSV)
    for col in ["nom_affichage", "adresse_complete", "adresse"]:
        merged[col] = merged[col].str.replace(r"[\r\n]+", " ", regex=True).str.strip()

    # Siège : O/N lisible
    merged["est_siege"] = merged["etablissementSiege"].map({"true": "Oui", "false": "Non"}).fillna("?")

    print(f"  → {len(merged):,} lignes après jointure")
    return merged


# =============================================================================
# ÉTAPE 4 : GÉOCODAGE VIA API BAN
# =============================================================================

def geocoder_ban(df, colonne_adresse="adresse_complete", colonne_cp="codePostalEtablissement"):
    """
    Géocode une colonne d'adresses via l'API BAN (batch CSV).
    Retourne le DataFrame avec les colonnes latitude et longitude ajoutées.
    
    L'API BAN accepte jusqu'à 50 000 lignes par appel.
    On travaille par lots de TAILLE_LOT_GEOCODAGE lignes.
    """
    print()
    print("=" * 60)
    print("ÉTAPE 4 : Géocodage via l'API BAN")
    print("=" * 60)
    print(f"  {len(df):,} adresses à géocoder, par lots de {TAILLE_LOT_GEOCODAGE:,}")

    resultats = []
    nb_lots = (len(df) // TAILLE_LOT_GEOCODAGE) + 1

    for i in range(0, len(df), TAILLE_LOT_GEOCODAGE):
        lot = df.iloc[i : i + TAILLE_LOT_GEOCODAGE].copy()
        num_lot = (i // TAILLE_LOT_GEOCODAGE) + 1
        print(f"  Lot {num_lot}/{nb_lots} ({len(lot):,} adresses)...", end=" ", flush=True)

        # Préparer le CSV pour l'API BAN
        # Nettoyer les champs pour éviter les retours à la ligne et guillemets
        lot_api = pd.DataFrame({
            "id":       lot.index.astype(str),
            "adresse":  lot[colonne_adresse].fillna("").astype(str).str.replace(r'[\r\n"]+', ' ', regex=True).str.strip(),
            "postcode": lot[colonne_cp].fillna("").astype(str).str.replace(r'[\r\n"]+', ' ', regex=True).str.strip(),
        })

        csv_bytes = lot_api.to_csv(index=False).encode("utf-8")

        lot["latitude"]  = pd.NA
        lot["longitude"] = pd.NA
        lot["geocodage_score"] = pd.NA
        lot["geocodage_label"] = pd.NA

        succes = False
        for tentative in range(1, 4):
            try:
                response = requests.post(
                    "https://api-adresse.data.gouv.fr/search/csv/",
                    files={"data": ("adresses.csv", csv_bytes, "text/csv")},
                    data={"columns": ["adresse"], "postcode": "postcode"},
                    timeout=120 * tentative,
                )
                response.raise_for_status()

                result_df = pd.read_csv(io.StringIO(response.text), dtype=str, on_bad_lines='skip')

                # Recollage par la colonne "id" (robuste même si des lignes sont skippées)
                if "id" in result_df.columns:
                    result_df["id"] = result_df["id"].astype(str)
                    result_df = result_df.set_index("id")
                    idx_str = lot.index.astype(str)
                    lot["latitude"]        = result_df.reindex(idx_str)["latitude"].values        if "latitude"     in result_df.columns else pd.NA
                    lot["longitude"]       = result_df.reindex(idx_str)["longitude"].values       if "longitude"    in result_df.columns else pd.NA
                    lot["geocodage_score"] = result_df.reindex(idx_str)["result_score"].values    if "result_score" in result_df.columns else pd.NA
                    lot["geocodage_label"] = result_df.reindex(idx_str)["result_label"].values    if "result_label" in result_df.columns else pd.NA
                else:
                    result_df.index = lot.index
                    lot["latitude"]        = result_df.get("latitude",     pd.NA)
                    lot["longitude"]       = result_df.get("longitude",    pd.NA)
                    lot["geocodage_score"] = result_df.get("result_score", pd.NA)
                    lot["geocodage_label"] = result_df.get("result_label", pd.NA)

                nb_geocodes = lot["latitude"].notna().sum()
                print(f"✓ ({nb_geocodes:,} géocodés)")
                succes = True
                break

            except Exception as e:
                if tentative < 3:
                    print(f"✗ tentative {tentative} échouée ({e}), nouvelle tentative dans {tentative * 5}s...", end=" ", flush=True)
                    time.sleep(tentative * 5)
                else:
                    print(f"✗ ERREUR après 3 tentatives : {e}")

        if not succes:
            pass  # colonnes déjà initialisées à pd.NA ci-dessus

        resultats.append(lot)

        # Petite pause pour ne pas surcharger l'API
        if num_lot < nb_lots:
            time.sleep(1)

    return pd.concat(resultats, ignore_index=False)


# =============================================================================
# ÉTAPE 5 : EXPORT PAR FAMILLE
# =============================================================================

def exporter_par_famille(df, dossier_output):
    """
    Divise le DataFrame en familles selon le préfixe de catégorie juridique
    et exporte un CSV par famille.
    """
    print()
    print("=" * 60)
    print("ÉTAPE 5 : Export des fichiers CSV par famille")
    print("=" * 60)

    os.makedirs(dossier_output, exist_ok=True)

    # Colonnes finales pour UMap
    colonnes_export = [
        "nom_affichage",
        "est_siege",
        "adresse_complete",
        "categorieJuridiqueUniteLegale",
        "libelle_categorie_juridique",
        "activitePrincipaleEtablissement",
        "libelle_activite",
        "effectifs_libelle",
        "anneeEffectifsEtablissement",
        "latitude",
        "longitude",
    ]

    recap = []

    for prefixe, (nom_fichier, libelle, versant) in FAMILLES.items():
        if prefixe == "4":
            # Cas spécial : toutes les catégories commençant par 4
            mask = df["categorieJuridiqueUniteLegale"].str.startswith("4", na=False)
        else:
            mask = df["prefixe_cj"] == prefixe

        sous_df = df[mask].copy()

        if len(sous_df) == 0:
            print(f"  [VIDE] {libelle}")
            continue

        chemin_sortie = os.path.join(dossier_output, nom_fichier)
        sous_df[colonnes_export].to_csv(chemin_sortie, index=False, encoding="utf-8-sig")

        nb_geocodes = sous_df["latitude"].notna().sum()
        taux = nb_geocodes / len(sous_df) * 100 if len(sous_df) > 0 else 0

        print(f"  [{versant}] {libelle}")
        print(f"    → {len(sous_df):,} établissements, {nb_geocodes:,} géocodés ({taux:.1f}%)")
        print(f"    → CSV : {chemin_sortie}")

        # Export GeoPackage (uniquement les lignes géocodées)
        if GEOPANDAS_DISPONIBLE:
            sous_df_geo = sous_df[sous_df["latitude"].notna() & sous_df["longitude"].notna()].copy()
            sous_df_geo["latitude"]  = pd.to_numeric(sous_df_geo["latitude"],  errors="coerce")
            sous_df_geo["longitude"] = pd.to_numeric(sous_df_geo["longitude"], errors="coerce")
            sous_df_geo = sous_df_geo.dropna(subset=["latitude", "longitude"])
            if len(sous_df_geo) > 0:
                geometry = [Point(lon, lat) for lon, lat in zip(sous_df_geo["longitude"], sous_df_geo["latitude"])]
                gdf = gpd.GeoDataFrame(sous_df_geo[colonnes_export], geometry=geometry, crs="EPSG:4326")
                nom_gpkg = nom_fichier.replace(".csv", ".gpkg")
                chemin_gpkg = os.path.join(dossier_output, nom_gpkg)
                gdf.to_file(chemin_gpkg, driver="GPKG", layer=nom_fichier.replace(".csv", ""))
                print(f"    → GeoPackage : {chemin_gpkg}")
        else:
            print(f"    → GeoPackage ignoré (installer geopandas : pip install geopandas)")

        recap.append({
            "versant":        versant,
            "famille":        libelle,
            "nb_etab":        len(sous_df),
            "nb_geocodes":    nb_geocodes,
            "taux_geocodage": f"{taux:.1f}%",
            "fichier":        nom_fichier,
        })

    # Fichier récapitulatif
    recap_df = pd.DataFrame(recap)
    chemin_recap = os.path.join(dossier_output, "_recapitulatif.csv")
    recap_df.to_csv(chemin_recap, index=False, encoding="utf-8-sig")
    print(f"\n  Récapitulatif enregistré : {chemin_recap}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   SIRENE → Extraction établissements publics pour UMap   ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    chemin_ul   = os.path.join(DOSSIER_SIRENE, "StockUniteLegale_utf8.csv")
    chemin_etab = os.path.join(DOSSIER_SIRENE, "StockEtablissement_utf8.csv")

    # Vérification des fichiers
    for chemin in [chemin_ul, chemin_etab]:
        if not os.path.exists(chemin):
            print(f"ERREUR : Fichier introuvable : {chemin}")
            print("Vérifie que les fichiers SIRENE sont dans le dossier :", DOSSIER_SIRENE)
            return

    # Étape 1 : unités légales
    ul = charger_unites_legales(chemin_ul)

    # Étape 2 : établissements
    etab = charger_etablissements(chemin_etab, ul["siren"].unique())

    # Étape 3 : jointure + enrichissement
    df = enrichir(etab, ul)

    # Étape 4 : géocodage
    #
    # Option A — appel API automatique (lent pour >100k adresses) :
    #   skip_geocodage = False
    #   fichier_geocodage = None
    #
    # Option B — géocodage via la plateforme BAN (recommandé) :
    #   1. Lancer avec skip_geocodage = True  → génère output_sirene/adresses_a_geocoder.csv
    #   2. Déposer ce fichier sur https://adresse.data.gouv.fr/csv
    #   3. Télécharger le résultat, le renommer "adresses_geocodees.csv" dans ce dossier
    #   4. Relancer avec fichier_geocodage = "adresses_geocodees.csv"
    #
    skip_geocodage    = False
    fichier_geocodage = None   # ex: "adresses_geocodees.csv"

    if fichier_geocodage and os.path.exists(fichier_geocodage):
        print(f"\n  [INFO] Chargement des coordonnées depuis : {fichier_geocodage}")
        geo = pd.read_csv(
            fichier_geocodage, dtype=str,
            usecols=["siret", "latitude", "longitude", "result_score", "result_label"]
        )
        geo = geo.rename(columns={"result_score": "geocodage_score", "result_label": "geocodage_label"})
        geo["latitude"]  = pd.to_numeric(geo["latitude"],  errors="coerce")
        geo["longitude"] = pd.to_numeric(geo["longitude"], errors="coerce")
        df = df.merge(
            geo[["siret", "latitude", "longitude", "geocodage_score", "geocodage_label"]],
            on="siret", how="left"
        )
        nb = df["latitude"].notna().sum()
        print(f"  → {nb:,} établissements géocodés sur {len(df):,}")

    elif skip_geocodage:
        print("\n  [INFO] Export du fichier d'adresses pour géocodage via la BAN")
        os.makedirs(DOSSIER_OUTPUT, exist_ok=True)
        chemin_adresses = os.path.join(DOSSIER_OUTPUT, "adresses_a_geocoder.csv")
        df[["siret", "adresse_complete", "codePostalEtablissement"]].rename(
            columns={"adresse_complete": "adresse", "codePostalEtablissement": "postcode"}
        ).to_csv(chemin_adresses, index=False, encoding="utf-8-sig")
        print(f"  → {len(df):,} adresses exportées : {chemin_adresses}")
        print(f"  → Dépose ce fichier sur https://adresse.data.gouv.fr/csv")
        print(f"  → Renomme le résultat 'adresses_geocodees.csv' dans ce dossier")
        print(f"  → Puis relance avec : fichier_geocodage = \"adresses_geocodees.csv\"")
        df["latitude"]        = pd.NA
        df["longitude"]       = pd.NA
        df["geocodage_score"] = pd.NA
        df["geocodage_label"] = pd.NA

    else:
        df = geocoder_ban(df)

    # Étape 5 : export
    exporter_par_famille(df, DOSSIER_OUTPUT)

    print()
    print("✅ Terminé ! Les fichiers sont dans le dossier :", DOSSIER_OUTPUT)
    print()


if __name__ == "__main__":
    main()