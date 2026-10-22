# Guide du projet SciPulse

## Introduction

Le projet SciPulse a pour objectif d'explorer un pipeline de traitement de données (un « ETL ») autour de la pile **Elastic Stack**.
Celle-ci se compose de trois éléments distincts :
- **Elasticsearch** : l'espace de stockage + un moteur de recherche dérivé de **Lucene** ;
- **Logstash**  : Un outils d'ingestion et de traitement des données ;
- **Kibana** : un outil de visualisation et d'inspection de données

Au-delà de ce noyau, le projet utilise aussi :
- **MinIO** : un outil de stockage répliquant des buckets S3 d'AWS sur un disque local
- **PostgreSQL** : une base de données relationnelle (en réalité, désormais, multi-paradigmes)

## Le projet

L'objectif du projet est de mettre sur pied un outil d'analyse de texte pour la veille scientifique.
Nous voulons examiner le corpus d'articles déposés sur le site [ArXiv](https://arxiv.org), une immense base de documents scientifiques en pré-publication, c'est-à-dire à une étape où les résultats de recherche n'ont pas encore été relus par des pairs.
A ce stade, les articles n'ont pas encore été acceptés par des revues scientifiques et ne sont donc pas soumis au droit (et aux frais !) d'accès.

**ArXiv** étant un dépôt qui rassemble de nombreuses disciplines, nous voudrons nous limiter à un partie des ressources, par exemple celle qui concerne l'informatique et l'apprentissage automatique.

Pour établir la portée de l'exploration, nous choisirons certaines catégories, ou domaines de recherche, qu'ArXiv appelle des **contextes**. Ceux-ci sont référencés par des codes, comme `cs` pour l'informatique (Computer Science) ou `math` pour les mathématiques, ou `econ`pour l'économie.

Dans sa forme définitive, notre projet devrait scruter les nouvelles publications du domaine choisi en consultant l'API d'ArXiv (il existe un module Python pour cela), mais dans un premier temps, nous voulons constituer une base qui reprenne l'historique des publications.
Heureusement, il existe une archive, consituée par l'université Cornell, qui contient un grand nombre d'article déjà déposés. Cette archive peut être téléchargée dpuis **Kaggle**.

En complément, nous voudrons établir une veille sur le site [Hacker News](https://news.ycombinator.com), afin de collecter des informations, des commentaires, ou encore des éléments d'actualité sur les projets de recherche publiés.

Une fois cette archive installée, les différentes phases du projet consisteront à :

1. Installer l'ensemble des outils logiciels nécessaires et vérifier leur bon fonctionnement
2. Comprendre la structure du jeu de données initial
3. Importer les données brutes dans un espace de stockage initial (en l'occurrence MinIO)
4. Modéliser les données de manière à configurer Elasticsearch
5. Mettre en œuvre un processus d'examen de la qualité des données (parenthèse DataOps)
6. Transformer les données pour les projeter dans Elasticsearch
7. Interroger les données avec Kibana
8. Orchester le tout avec Airflow
9. Eventuellement, tracer des pistes d'amélioration du pipeline

## Démarrage rapide

Prérequis : Docker Desktop (≥ 8 Go de RAM alloués), Python ≥ 3.10.

```bash
python -m venv env && source env/bin/activate
pip install -r requirements.txt

docker compose up -d elasticsearch kibana postgres minio minio-init logstash
# (+ airflow-webserver airflow-scheduler pour l'orchestration, 1er build ≈ 3 min)

# Données : échantillon synthétique (2 000 articles) ou vrai dump Kaggle
python -m src.ingestion.generate_sample --n 2000
#   ou : kaggle datasets download -d Cornell-University/arxiv --unzip -p data/
#        python -m src.ingestion.prepare_dump data/arxiv-metadata-oai-snapshot.json data/arxiv-raw/arxiv-cs-subset.json --limit 100000
```

Logstash ingère automatiquement `data/arxiv-raw/*.json` dans `arxiv-papers-raw` (ou utilisez
`python -m src.ingestion.arxiv_bulk_ingest FICHIER`). Ensuite, étape par étape :

```bash
python -m src.utils.mappings                  # index + mappings explicites
python -m src.ingestion.upload_to_minio data/arxiv-raw/arxiv-sample.json   # landing zone S3
python -m src.ingestion.reindex_arxiv         # raw -> arxiv-papers (analyseur scientific_english, nested…)
python -m src.quality.validate_arxiv          # Great Expectations (Data Docs : gx/uncommitted/data_docs)
python -m src.quality.clean_arxiv [--dry-run] # nettoyage guidé par les validations
python -m src.ingestion.hn_poller             # Hacker News (--continuous pour boucler)
python -m src.quality.validate_hn
python -m src.enrichment.detect_arxiv_links   # liens ArXiv dans les posts HN -> arxiv-hn-links
python -m src.enrichment.enrich_arxiv         # TF-IDF keywords + hn_score / hn_comments_count
python -m src.transform.load_postgres         # miroir PostgreSQL
(cd dbt && dbt seed --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir .)
python -m src.viz.setup_kibana                # dashboards « SciPulse » et « Monitoring »
python -m src.viz.monitoring                  # contrôle de santé (fraîcheur HN, taux d'échec)
```

Ou tout d'un coup via Airflow (http://localhost:8080, `admin` / `admin`) : dépauser puis déclencher
`scipulse_arxiv_pipeline` (batch, manuel) et `scipulse_hn_poller` (toutes les 5 min).

| Service | URL |
|---|---|
| Kibana (dashboard : `/app/dashboards#/view/scipulse-dashboard`) | http://localhost:5601 |
| Elasticsearch | http://localhost:9200 |
| Airflow | http://localhost:8080 |
| MinIO console | http://localhost:9001 (`minioadmin` / `minioadmin`) |
| PostgreSQL | `localhost:${PG_PORT:-5432}` |

### Recherche

```python
from src.search.search_service import SearchService
svc = SearchService()
svc.search("reinforcement learning robotics", categories=["cs.RO"])   # multi_match + function_score + highlight
svc.more_like_this("2301.07041")
svc.significant_terms(category="cs.CR")
svc.suggest("convlutional nueral network")
svc.hn_posts_with_comment("breakthrough")                            # has_child
```

### Tests

```bash
pytest                    # tests unitaires (sans Docker)
pytest -m integration     # + Elasticsearch réel (index dédiés it-*, n'affecte pas vos données)
```

## Architecture du dépôt

```
airflow/dags/       DAGs : scipulse_arxiv_pipeline (batch), scipulse_hn_poller (micro-batch)
dbt/                modèles staging + marts (dim_categories, fct_publications_par_mois, fct_auteurs_prolifiques…)
docker/             logstash (pipeline arxiv.conf), postgres (init.sql), airflow (image + deps), minio, kibana (export ndjson)
gx/                 projet Great Expectations (suites, checkpoints ; Data Docs non versionnées)
src/ingestion/      MinIO, Logstash-équivalent Python, réindexation, poller HN, générateur d'échantillon
src/quality/        extraction, validations GX (ArXiv, HN), nettoyage
src/enrichment/     TF-IDF, liens ArXiv ↔ HN, scores croisés
src/search/         search_service.py
src/transform/      miroir PostgreSQL
src/viz/            dashboards Kibana, monitoring, alerte Watcher
src/utils/          config, mappings, journal pipeline-logs
tests/              pytest
Chapitre_0x_*/      supports de cours (PAD / MEM / TP)
```

## Notes d'exploitation

- **MinIO** n'est plus publié sur Docker Hub ni sur quay.io (accès anonyme) : `docker/minio/Dockerfile`
  compile le serveur et `mc` depuis les sources à une version figée (build ≈ 2 min la première fois).
- **PostgreSQL local déjà installé ?** Il occupe `localhost:5432` et masque le conteneur. Lancez
  `PG_PORT=5433 docker compose up -d` et exportez `PG_PORT=5433` côté Python / dbt.
- **Great Expectations ≥ 1.0** (API `gx.get_context`, `ValidationDefinition`, `Checkpoint`) — les énoncés de TP
  mentionnent encore l'API 0.18 ; l'esprit (suites d'expectations → checkpoint → Data Docs) est identique.
- **Watcher** (alertes ES) nécessite une licence d'essai : `python -m src.viz.monitoring --watcher`.
- Le dump complet n'a été exécuté ici que sur l'échantillon synthétique ; les performances à 500 k articles
  (shards, bulk size, heap ES) restent à valider sur votre machine.

## Ce que vous trouverez ici :

### Docker

Le fichier `docker-compose.yml` contient normalement tout ce qu'il faut pour pouvoir lancer les outils de manière fluide.

Dans un premier temps, la solution choisie est « hybride », c'est-à-dire que les scripts Python (et Python lui-même) sont installés sur la machine locale et seules les applications sont encapsulées dans des conteneurs.

En fin de parcours, iserait peut-être nécessaire de tout mettre dans un espace virtuel Docker.

### Les supports

Vous trouverez un certain nombre de fichier préfixés par `PAD-xx`. Ce sont les rappels théoriques à propos de toutes les étapes apr lesquelles vous devrez passer pour achever le projet. La numérotation correspond à des chapitres du projet. Elle est uniforme pour tous les types de fichiers.

### Les énoncés de TP

Tous les fichiers préfixés `TP-xxE` sont les énoncés des exercices à faire.

### Les mémentos

Tous les fichiers préfixés `MEM-xx` sont des mémentos récapitulant les ressources — modules, fonctons, etc. — dont vous aurez besoin pourécrire le code de chaque étape du projet.

### Le syllabus complet du cours :

- [Syllabus](Syllabus_ES.md)
