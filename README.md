# Carte des établissements publics français — Extraction SIRENE

Extraction automatisée des établissements publics (FPE, FPT, FPH) à partir des fichiers SIRENE de l'INSEE, avec géocodage via l'API BAN et export en CSV et GeoPackage pour visualisation sur UMap ou cartes.gouv.fr.

## Objectif

Produire des couches géographiques réutilisables des établissements publics français, destinées à être :
- visualisées sur [UMap](https://umap.openstreetmap.fr) ou [cartes.gouv.fr](https://cartes.gouv.fr)
- publiées en open data sur [data.gouv.fr](https://www.data.gouv.fr)

## Sources de données

| Source | Description | Licence | Lien |
|--------|-------------|---------|------|
| **SIRENE StockEtablissement** | Fichier stock des établissements (tous secteurs) | Licence Ouverte 2.0 | [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/) |
| **SIRENE StockUniteLegale** | Fichier stock des unités légales (catégorie juridique, dénomination) | Licence Ouverte 2.0 | [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/) |
| **Nomenclature catégories juridiques INSEE** | Table de référence des codes catégories juridiques (ex : `cj_septembre_2022.xls`) | Licence Ouverte | [insee.fr](https://www.insee.fr/fr/information/2028129) |
| **API BAN** (Base Adresse Nationale) | Géocodage des adresses postales | Licence Ouverte 2.0 | [adresse.data.gouv.fr](https://adresse.data.gouv.fr) |

Les fichiers SIRENE (`StockEtablissement_utf8.csv` et `StockUniteLegale_utf8.csv`) ne sont **pas inclus** dans ce dépôt — ils font plusieurs gigaoctets et sont disponibles en téléchargement libre sur data.gouv.fr (liens ci-dessus).

## Prérequis

- Python 3.9+
- Bibliothèques Python :

```bash
pip install pandas requests geopandas
```

> `geopandas` est nécessaire uniquement pour l'export GeoPackage. Si vous n'en avez pas besoin, l'installation de `pandas` et `requests` suffit.

## Fichiers nécessaires

Placer dans le même dossier que le script :

```
sirene_fpe_extraction.py
StockEtablissement_utf8.csv     ← téléchargé depuis data.gouv.fr
StockUniteLegale_utf8.csv       ← téléchargé depuis data.gouv.fr
```

## Utilisation

### Mode simple (géocodage automatique via API BAN)

```bash
python sirene_fpe_extraction.py
```

Le script appelle l'API BAN par lots de 3 000 adresses. Adapté pour des volumes modérés (jusqu'à ~50 000 établissements). Pour les gros volumes (90 000+ lignes), préférer le mode batch ci-dessous.

### Mode batch BAN (recommandé pour les gros volumes)

1. Dans le script, passer `skip_geocodage = True`
2. Lancer le script → génère `output_sirene/adresses_a_geocoder.csv`
3. Déposer ce fichier sur [adresse.data.gouv.fr/csv](https://adresse.data.gouv.fr/csv)
4. Télécharger le résultat, le renommer `adresses_geocodees.csv` dans le dossier
5. Dans le script, renseigner `fichier_geocodage = "adresses_geocodees.csv"` et relancer

## Déroulé du script

```
ÉTAPE 1 — Chargement des unités légales
         Lecture de StockUniteLegale_utf8.csv (~1 Go)
         Filtrage sur les catégories juridiques publiques (4x, 71xx–76xx)
         Résultat : ~X unités légales actives

ÉTAPE 2 — Chargement des établissements
         Lecture de StockEtablissement_utf8.csv (~4–5 Go)
         Filtrage sur les établissements actifs dont le SIREN est public
         Résultat : ~90 000 établissements

ÉTAPE 3 — Jointure et enrichissement
         Croisement établissements ↔ unités légales (par SIREN)
         Ajout des libellés : catégorie juridique, activité NAF, effectifs
         Construction de l'adresse complète

ÉTAPE 4 — Géocodage via API BAN
         Envoi des adresses par lots à l'API adresse.data.gouv.fr
         Récupération des coordonnées latitude / longitude

ÉTAPE 5 — Export
         Un fichier CSV + un fichier GeoPackage par famille (versant)
```

## Familles produites

| Fichier | Versant | Description |
|---------|---------|-------------|
| `fpe_71_etat_services.csv/.gpkg` | FPE | État : ministères, directions, services déconcentrés, juridictions |
| `fpe_73_ep_nationaux.csv/.gpkg` | FPE | Établissements publics nationaux (universités, CEREMA, ARS…) |
| `fpe_74_gip_divers.csv/.gpkg` | FPE | GIP, établissements culturels, armées… |
| `fpe_4x_operateurs_epic.csv/.gpkg` | FPE | Opérateurs et EPIC (SNCF, ADEME, IGN…) |
| `fph_75_secu_sociale.csv/.gpkg` | FPH | Organismes de sécurité sociale (CPAM, CAF, URSSAF…) |
| `fph_76_autres_sociaux.csv/.gpkg` | FPH | Autres organismes sociaux (CCAS, CIAS…) |

## Colonnes des fichiers de sortie

| Colonne | Description |
|---------|-------------|
| `nom_affichage` | Dénomination de l'établissement |
| `est_siege` | Oui / Non — permet de filtrer sur le siège social |
| `adresse_complete` | Adresse postale complète |
| `categorieJuridiqueUniteLegale` | Code INSEE à 4 chiffres |
| `libelle_categorie_juridique` | Libellé lisible de la catégorie juridique |
| `activitePrincipaleEtablissement` | Code NAF / APE |
| `libelle_activite` | Libellé de l'activité |
| `effectifs_libelle` | Tranche d'effectifs salariés |
| `anneeEffectifsEtablissement` | Année de référence des effectifs |
| `latitude` | Coordonnée géographique (WGS84) |
| `longitude` | Coordonnée géographique (WGS84) |

## Utilisation sur cartes.gouv.fr / UMap

Les fichiers GeoPackage (`.gpkg`) peuvent être importés directement sur [cartes.gouv.fr](https://cartes.gouv.fr) en tant que couches géographiques. Les fichiers CSV peuvent être utilisés sur UMap en tant que calques de données distants (URL) ou importés manuellement.

La colonne `est_siege` permet de filtrer les points pour n'afficher que les sièges et alléger l'affichage.

## Licence

Code et données produites publiés sous **Licence Ouverte / Open Licence 2.0 (Etalab)**.
Réutilisation libre, y compris commerciale, avec mention de la source.
Voir le fichier [LICENSE](LICENSE).

Les données sources (SIRENE, BAN) sont elles-mêmes publiées sous Licence Ouverte 2.0 par l'INSEE et la DINUM.

## Auteur

Contribution bienvenue. Projet proposé à la réutilisation sur [data.gouv.fr](https://www.data.gouv.fr).
