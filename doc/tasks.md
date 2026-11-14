# Plan du Projet & Suivi des Tâches (`tasks.md`)

Ce plan distingue clairement le **périmètre MVP** (mono-utilisateur à l'exécution mais reposant dès le Jour 1 sur un modèle de données multi-utilisateurs avec `user_id` `UUID` non auto-incrémenté) et l'**évolution Post-MVP** (ouverture multi-locataire avec OIDC obligatoire et durcissement RLS).

---

## Phase 0 : Cadrage, Choix Technologiques & Architecture Initiale
- [x] Clarifier les besoins fonctionnels et non-fonctionnels (sources multiples, abonnements payants, recommandations "Homes", auto-profilage, quotas OpenRouter `:free`, cible S3/K8s, phasage MVP vs OIDC Post-MVP).
- [x] Vérifier les contraintes des APIs et briques techniques auprès des documentations officielles et analyser l'articulation avec le projet frère `agent-auth-mcp`.
- [x] Rédiger les décisions d'architecture sourcées dans [`doc/adr.md`](adr.md).
- [x] Définir le plan d'exécution dans [`doc/tasks.md`](tasks.md), initialiser [`README.md`](../README.md), [`.gitignore`](../.gitignore), [`.env.example`](../.env.example) et poser l'arborescence du dépôt.

---

# Périmètre MVP (Socle Data Multi-User Ready `user_id: UUID`, Exécution `single_user`)

## Phase 1 : Socle Local, Schéma PostgreSQL (`UUID` + `pgvector` + `FTS`) & Stockage S3
- [x] **Environnement local (`docker-compose.yml`)** :
  - Déclarer PostgreSQL 16/17 avec `pgvector` (`pgvector/pgvector:pg16`) et un stockage objet local compatible S3 (`chrislusf/seaweedfs:latest` `server -s3`).
- [x] **Initialisation Backend Python (`pyproject.toml` via `uv`)** :
  - Configurer `fastapi`, `uvicorn`, `sqlalchemy`, `psycopg[binary]`, `pgvector`, `boto3`, `pydantic-settings`, `cryptography`, `fastembed`, `trafilatura`, `feedparser`.
  - Implémenter `morning_review/core/config.py` et la dépendance `get_current_user_id() -> UUID` en mode `AUTH_MODE=single_user` (seed automatique de `BOOTSTRAP_USER_ID`).
- [x] **Modèle de données PostgreSQL (`morning_review/db/`)** :
  - Clés primaires et étrangères en **`UUID` natif PostgreSQL** (aucun auto-incrément sur `users` ni sur les entités métier).
  - Tables mutualisées : `sources`, `topics` (`id` UUID, `centroid_embedding halfvec(384)`, `fts tsvector`, `summary`), `topic_items`.
  - Tables rattachées à `user_id: UUID` : `users` (incluant déjà `oidc_issuer`, `oidc_sub` nullables), `user_categories`, `user_subscriptions`, `canonical_items` (`visibility`, `user_id` nullable si `public` / obligatoire si `private`, `embedding halfvec(384)`, `embedding_model_version`, `fts tsvector`, `s3_key`), `source_mentions`, `user_topic_feedback`, `user_profiles`, `user_digests`.
  - Index `HNSW (halfvec_cosine_ops)` sur les vecteurs et `GIN` sur les `tsvector`.
- [x] **Client Stockage Objet S3 (`morning_review/core/storage.py`)** :
  - Écriture/lecture adressable par `SHA-256` dans `public/items/...` (sources publiques mutualisées) et `users/<user_id>/items/...` (sources privées/payantes).
- [x] **Validation Phase 1** :
  - Tests `pytest` validant le seed idempotent de `BOOTSTRAP_USER_ID`, le filtrage par `user_id` (`UUID`) et le stockage S3 dédoublonné.

---

## Phase 2 : API Fondamentale Sources/Catégories & Connecteurs Serveur (RSS, Web, Gmail IMAP)
- [x] **Maintien de la liste des flux & ressources (`morning_review/connectors/source_registry.py` & `config/sources.yaml`)** :
  - Synchronisation bidirectionnelle entre [`config/sources.yaml`](../config/sources.yaml) et les tables PostgreSQL `user_categories`, `sources` et `user_subscriptions`.
  - Auto-inscription des newsletters découvertes dans Gmail (`kind: newsletter`, `discovery_method: gmail_auto`) et endpoints REST `/api/v1/sources`.
- [x] **Normalisation & Extraction Déterministe (`morning_review/pipeline/ingestion.py`)** :
  - Normalisation d'URL canonique (retrait `utm_*`, `mc_cid`, `fbclid`...) et extraction HTML $\rightarrow$ Markdown via `Trafilatura`.
- [x] **Connecteur RSS / Atom (`morning_review/connectors/rss.py`)** :
  - Collecte `httpx` conditionnelle (`ETag` / `If-Modified-Since` `304`) et `feedparser`.
- [x] **Connecteur Gmail IMAP & Déplieur de Newsletters (`morning_review/connectors/gmail_imap.py`)** :
  - Connexion TLS `imap.gmail.com:993` lisant `GMAIL_ACCOUNT`/`GMAIL_PWD`, détection déterministe des newsletters (en-têtes RFC 2369/2919 `List-Unsubscribe`, `List-Id`, `Feedback-ID`, motifs d'expéditeurs) et extraction unitaire des sujets/liens curés.
- [ ] **Ordonnancement périodique `Procrastinate` (`morning_review/workers/`)** :
  - Tâches périodiques cron de relève automatique RSS/Web et Gmail IMAP.
- [x] **Validation Phase 2** :
  - Tests unitaires et d'intégration sur flux RSS et emails HTML de newsletters de curation multi-liens (`backend/tests/test_mvp_flow.py`).

---

## Phase 3 : Intégration `agent-auth-mcp` & Collecteur Edge (`mr-edge-sync` : Paywalls & "Homes")
- [ ] **Endpoint d'ingestion privée (`POST /api/v1/ingest/edge-batch`)** :
  - Réception par lots d'items Markdown pré-extraits rattachés au `user_id` courant (`visibility = 'private'`, stockage S3 sous `users/<user_id>/items/...`).
- [ ] **Agent local `edge_sync` (`edge_sync/src/mr_edge_sync/`)** :
  - Client `fastmcp.Client` (`StdioTransport` forcé en `--mode read-only`) interrogeant les serveurs MCP disponibles dans `agent-auth-mcp` lorsque leurs `.token` en RAM (`/run/user/$UID/agent-tokens/`) sont valides.
  - Intégration immédiate des connecteurs existants dans `agent-auth-mcp` : `pourlascience`, `newscientist`, `medium` (et optionnellement `notebooklm` / `notion`).
  - Unités `systemd --user` (`mr-edge-sync.service` et `mr-edge-sync.timer`) avec curseur local anti-doublon.
- [ ] **Extensions coordonnées dans le projet frère `agent-auth-mcp/tools/`** :
  - Presse abonnée : `canardpc` (`auth.py`, `client.py`, `mcp_server.py` `read-only`, `api-contract.md`).
  - Recommandations d'accueil `read-only` : `youtube_home`, `spotify_home`, `x_home` (et `facebook_feed` optionnel).
- [ ] **Validation Phase 3** :
  - Test de bout en bout entre un serveur FastMCP `read-only`, `mr-edge-sync` et l'API d'ingestion privée.

---

## Phase 4 : Pipeline Analytique (Embeddings ONNX, `topic_id`, Recherche Hybride, Profilage & OpenRouter)
- [ ] **Vectorisation CPU Locale (`morning_review/pipeline/embeddings.py`)** :
  - Intégration de `FastEmbed` avec le modèle natif `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` (ONNX Runtime CPU, 384d, 220 Mo), chargement *lazy* côté API, limitation `OMP_NUM_THREADS=2` et tâche de re-vectorisation basée sur `embedding_model_version` depuis S3.
- [ ] **Identification & Clustering en `topic_id` unique (`morning_review/pipeline/clustering.py`)** :
  - Rattachement déterministe par URL primaire citée ou voisinage cosinus HNSW (`halfvec_cosine_ops`, `>= TOPIC_SIMILARITY_THRESHOLD` sur `TOPIC_WINDOW_DAYS`), mise à jour du centroïde `topics.centroid_embedding`.
- [ ] **Moteur de Recherche Hybride (`morning_review/api/routes/search.py`)** :
  - Requête SQL filtrée sur `(visibility = 'public' OR user_id = :user_id)` combinant `tsvector` (`GIN`) et `pgvector` `halfvec(384)` (`HNSW`) via **Reciprocal Rank Fusion (RRF, $k=60$)** (`hybrid`, `keyword`, `semantic`).
- [ ] **Auto-Profilage par `user_id` (`morning_review/pipeline/profiling.py`)** :
  - Endpoints de vote (`Pertinent` / `Pas pertinent` via l'UI Web et via lien signé HMAC dans l'email).
  - Mise à jour du vecteur `user_profiles.interest_embedding` par algorithme de **Rocchio** pondéré temporellement + affinités bayésiennes sources/catégories ; calcul du `relevance_score`.
- [ ] **Synthèse LLM sous Quota OpenRouter `:free` (`morning_review/pipeline/llm_openrouter.py`)** :
  - Rate-limiter PostgreSQL (`<= 15 RPM`, `<= 900 RPD`), suivi `/api/v1/key`, synthèse mutualisée sur `topics` publics, respect du flag `allow_external_llm` et fallback déterministe automatique en cas de `HTTP 429`.
- [ ] **Validation Phase 4** :
  - Tests de regroupement multi-sources sous un même `topic_id`, recherche hybride FR/EN, évolution du `relevance_score` après feedbacks et respect des quotas OpenRouter.

---

## Phase 5 : Génération & Diffusion Tri-Canal du Morning Review (Email, API Web, Dépôt Git)
- [ ] **Assembleur Quotidien (`morning_review/delivery/digest_builder.py`)** :
  - Sélection quotidienne par `user_id` et par catégorie (`tech`, `perso`...), exclusion des `topic_id` déjà envoyés, persistance dans `user_digests` et sur S3 (`users/<user_id>/digests/YYYY/MM/YYYY-MM-DD.md`).
- [ ] **Canal Email (`morning_review/delivery/email_sender.py`)** :
  - Rendu Jinja2 `HTML` + `Plaintext` avec synthèse par catégorie, liste des sources agrégées, lien Web persistant et liens HMAC `👍 Pertinent` / `👎 Pas pertinent` ; envoi SMTP (`aiosmtplib`).
- [ ] **Canal Dépôt Git (`morning_review/delivery/git_publisher.py`)** :
  - Commit et push automatisés du rapport Markdown quotidien (`reviews/YYYY/MM/YYYY-MM-DD.md` + index) sur le dépôt Git configuré.
- [ ] **Validation Phase 5** :
  - Génération complète d'une édition quotidienne, test d'envoi SMTP local et vérification du commit/push sur un dépôt Git bare de test.

---

## Phase 6 : Interface Web Persistante (`Vue.js 3`) & Packaging Kubernetes (`Kustomize`)
- [ ] **Application Web Vue.js 3 (`frontend/`)** :
  - Vue **Morning Review (`/`, `/reviews/:date`)** : lecture par catégories, calendrier d'archives persistantes, dépliage de toutes les sources d'un `topic_id` et lecteur Markdown S3 intégré.
  - Boutons rapides d'auto-profilage (`Pertinent` / `Pas pertinent` / `Favori`) et explication du score.
  - Vue **Recherche Hybride (`/search`)** : bascule `Hybride / Mot-clé / Sémantique` et filtres (catégories, sources, dates).
  - Vue **Sources & Paramètres (`/settings`)** : gestion des flux, catégories et déclenchement/suivi des collectes.
- [ ] **Conteneurisation & Manifestes Kustomize (`deploy/k8s/`)** :
  - `backend/Dockerfile` (Python 3.12 non-root, ONNX CPU avec pré-chargement au build des 220 Mo de `MiniLM-L12-v2` dans `FASTEMBED_CACHE_PATH=/opt/models` : 0 cold-start réseau) et `frontend/Dockerfile` (`nginxinc/nginx-unprivileged`).
  - `deploy/k8s/base/` (`api` en `uvicorn --workers 1` + `OMP_NUM_THREADS=2`, `worker`, `web`, `services`, `ingress`, `configmap`, `kustomization.yaml`) + `overlays/dev/` et `overlays/prod/`.
- [ ] **Validation Phase 6 (Fin du MVP)** :
  - Build Vue.js 3 sans erreur, validation `kubectl kustomize deploy/k8s/overlays/{dev,prod}` et test complet du MVP de bout en bout.

---

# Périmètre Post-MVP (Ouverture Multi-Utilisateurs & Durcissement Sécurité)

## Phase 7 : Authentification OIDC Obligatoire, Gestion Multi-Comptes & Row-Level Security (RLS)
- [ ] **Activation `AUTH_MODE=oidc` côté FastAPI (`morning_review/api/auth_oidc.py`)** :
  - Découverte automatique `/.well-known/openid-configuration`, cache `JWKS`, validation stricte des JWT (`iss`, `aud`, `exp`) sur tous les endpoints.
  - Provisionnement *Just-In-Time* dans `users` sur `(oidc_issuer, oidc_sub)` et rattachement automatique du compte historique `BOOTSTRAP_USER_ID` lors du premier login OIDC correspondant à `BOOTSTRAP_USER_EMAIL`.
- [ ] **Intégration `oidc-client-ts` (PKCE) dans Vue.js 3** :
  - Redirection OIDC obligatoire, callback PKCE, rafraîchissement des jetons et écran de gestion des Personal Access Tokens (`mr_pat_...` par `user_id` pour `mr-edge-sync`) et des connecteurs privés par utilisateur (IMAP/Git individuels).
- [ ] **Défense en profondeur PostgreSQL Row-Level Security (RLS)** :
  - Migration Alembic activant `ENABLE ROW LEVEL SECURITY` et les politiques d'isolation par `current_setting('app.current_user_id')::uuid` sur `canonical_items`, `source_mentions`, `user_categories`, `user_subscriptions`, `user_connectors`, `user_api_tokens`, `user_topic_feedback`, `user_profiles` et `user_digests`.
- [ ] **Validation Phase 7** :
  - Tests d'intrusion automatisés multi-locataires vérifiant l'impossibilité pour un `user_id` B de lire ou d'altérer les items privés, connecteurs, feedbacks ou rapports d'un `user_id` A, tout en partageant sans doublon les sources publiques.
