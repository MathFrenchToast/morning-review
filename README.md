# Morning Review

Plateforme personnelle et multi-utilisateurs de **veille informationnelle**, de **regroupement multi-sources par sujet (`topic_id`)**, d'**auto-profilage** et de génération quotidienne du **Morning Review** (Site Web persistant, Email matinal et archive Git).

---

## 1. Fonctionnalités clés

- **Agrégation multi-sources sans doublon** :
  - **Sources ouvertes (serveur)** : Flux RSS / Atom / JSON Feed, blogs et sites web publics (extraction déterministe Markdown via [Trafilatura](https://trafilatura.readthedocs.io/)).
  - **Newsletters de curation (serveur)** : Relève IMAP Gmail (`GMAIL_ACCOUNT` / `GMAIL_PWD` en MVP) avec découpage unitaire de chaque lien/sujet contenu dans une édition multi-articles.
  - **Abonnements payants & Recommandations externes "Homes" (Edge)** : Connexion en mode strictement `read-only` aux serveurs MCP du projet frère `agent-auth-mcp` (*Pour la Science*, *New Scientist*, *Medium*, puis *Canard PC*, *YouTube Home*, *Spotify Home*, *Twitter/X*) via l'agent local `mr-edge-sync`, sans exposer les jetons RAM `/run/user/$UID/agent-tokens/`.
- **Identification unique des sujets (`topic_id`) & Catégorisation** :
  - Chaque sujet reçoit un `topic_id` (`UUID` ordonné) fédérant les multiples sources qui traitent d'une même actualité (déduplication d'URL canonique + clustering cosinus `pgvector`).
  - Rangement par catégories (`tech`, `perso`, `science`...) personnalisables par utilisateur.
- **Recherche hybride a posteriori** :
  - Combinaison native dans PostgreSQL de la recherche plein texte par mots-clés (`tsvector` + index `GIN`) et de la recherche sémantique approchée (`pgvector` + index `HNSW`) fusionnées par **Reciprocal Rank Fusion (RRF)**.
- **Analyse déterministe + CPU ONNX + LLM économe (OpenRouter `:free`)** :
  - Embeddings multilingues calculés localement sur CPU via [FastEmbed](https://qdrant.github.io/fastembed/) (ONNX Runtime, sans GPU ni PyTorch).
  - Appels OpenRouter (`:free`, budget maîtrisé $\le 20\text{ RPM} / 1\,000\text{ RPD}$) réservés uniquement aux sujets regroupés et mutualisés sur les sources publiques, avec repli déterministe automatique en cas d'indisponibilité `HTTP 429`.
- **Auto-profilage continu** :
  - Boutons `👍 Pertinent` / `👎 Pas pertinent` (sur le Web et en 1 clic signé HMAC depuis l'email) ajustant dynamiquement le vecteur d'intérêt de l'utilisateur (algorithme de Rocchio pondéré dans le temps) et l'affinité par source.
- **Diffusion quotidienne tri-canal** :
  1. **Site Web persistant** (`Vue.js 3`) avec permaliens quotidiens, lecture intégrale et moteur de recherche hybride.
  2. **Email matinal** (`HTML` + `Plaintext`) structuré par catégories.
  3. **Dépôt Git** (`reviews/YYYY/MM/YYYY-MM-DD.md`) pour l'archivage pérenne en Markdown.
- **Modèle Multi-Utilisateurs dès le Jour 1 (`user_id: UUID`)** :
  - Toutes les entités dépendant de l'utilisateur portent un `user_id` de type **`UUID` natif PostgreSQL (unique et non auto-incrémenté)** et les contenus privés sont isolés sous `s3://<bucket>/users/<user_id>/...`, tandis que les sources publiques sont mutualisées dans `s3://<bucket>/public/...`.
  - **MVP** : Exécution légère en mode `AUTH_MODE=single_user` sur un `BOOTSTRAP_USER_ID` fixe sans friction OIDC.
  - **Post-MVP** : Activation d'**OIDC obligatoire** (Keycloak / Authentik / PocketID) et de **PostgreSQL Row-Level Security (RLS)** sans refonte du modèle de données.

---

## 2. Stack Technique

| Couche | Choix retenu | Rôle |
| :--- | :--- | :--- |
| **Backend & API** | Python 3.12+ (`uv`), FastAPI, SQLAlchemy 2.0, Alembic | API REST, modèle métier, validation d'accès |
| **Jobs & Planification** | Procrastinate (sur PostgreSQL) | Crons d'ingestion, file asynchrone sans Redis |
| **Base de données** | PostgreSQL 17 + `pgvector` (`HNSW`) + `tsvector` (`GIN`) | Stockage relationnel `UUID`, clustering `topic_id`, recherche hybride RRF |
| **Stockage texte** | Objet compatible S3 (`boto3` `endpoint_url`) | Stockage Markdown dédoublonné (`public/` & `users/<user_id>/`) |
| **Embeddings & IA** | FastEmbed (ONNX CPU multilingue 384d) + OpenRouter (`:free`) | Vectorisation locale gratuite + synthèse éditoriale économe |
| **Sources authentifiées** | `mr-edge-sync` + `agent-auth-mcp` (FastMCP `read-only`) | Presse abonnée & recommandations d'accueil personnelles |
| **Frontend** | Vue.js 3, Vite, TypeScript, Pinia (`+ oidc-client-ts` Post-MVP) | Consultation persistante, recherche hybride, auto-profilage |
| **Déploiement** | Kubernetes Kustomize (`base/` + `overlays/`) & Docker Compose | GitOps (ArgoCD / Flux) en prod & environnement local |

---

## 3. Arborescence du Dépôt

```text
morning-review/
├── README.md
├── .env.example                            # Variables d'environnement documentées (.env ignoré par Git)
├── .gitignore
├── doc/
│   ├── adr.md                              # Architecture Decision Records (vérifiés & sourcés)
│   └── tasks.md                            # Feuille de route MVP (Phases 1-6) & Post-MVP OIDC (Phase 7)
├── backend/
│   ├── migrations/                         # Migrations de schéma Alembic (PostgreSQL UUID + pgvector)
│   ├── src/morning_review/
│   │   ├── api/                            # Routes FastAPI, dépendance get_current_user_id()
│   │   ├── core/                           # Configuration, client S3 CAS, chiffrement Fernet
│   │   ├── db/                             # Modèles SQLAlchemy 2.0 (sources, items, topics, users)
│   │   ├── connectors/                     # RSS/Atom, Trafilatura Web, Gmail IMAP & parseur newsletters
│   │   ├── pipeline/                       # Embeddings FastEmbed ONNX, clustering topic_id, Rocchio, OpenRouter
│   │   ├── delivery/                       # Assembleur Morning Review, envoi SMTP, publication Git
│   │   └── workers/                        # Tâches asynchrones et crons Procrastinate
│   └── tests/                              # Tests unitaires et d'intégration pytest
├── edge_sync/
│   └── src/mr_edge_sync/                   # Collecteur local FastMCP read-only (agent-auth-mcp -> API)
├── frontend/
│   └── src/
│       ├── components/
│       ├── router/
│       ├── services/
│       ├── stores/
│       └── views/                          # Morning Review quotidien, Archives, Recherche hybride, Réglages
└── deploy/
    └── k8s/
        ├── base/                           # Manifestes déclaratifs Kustomize communs (api, worker, web)
        └── overlays/
            ├── dev/
            └── prod/
```

---

## 4. Documentation de Référence du Projet

- [Expression de besoin initiale (`doc/initial-prompt.md`)](doc/initial-prompt.md)
- [Décisions d'Architecture & Sources Officielles (`doc/adr.md`)](doc/adr.md)
- [Plan d'Exécution Détaillé MVP & Post-MVP (`doc/tasks.md`)](doc/tasks.md)
- [Configuration Type (`.env.example`)](.env.example)

---

## 5. Déploiement Kubernetes (K3s / ArgoCD) sur `vm-hostinger`

Le déploiement suit exactement le patron GitOps de `allbal` (`bal.perso.frenchtoast.dev`) pour exposer **`https://review.perso.frenchtoast.dev`** derrière **Traefik Gateway API**, **Cert-Manager (Let's Encrypt)** et une authentification **Traefik BasicAuth**, avec **CloudNativePG** (`postgresql:17.2` + `pgvector`), **Backblaze B2 S3** et deux **`CronJob` Kubernetes** (`Europe/Paris`).

### 5.1. Prérequis et Secrets (à exécuter une seule fois sur le cluster)

1. **Créer le namespace `morning-review`** :
   ```bash
   kubectl create namespace morning-review --dry-run=client -o yaml | kubectl apply -f -
   ```

2. **Copier le PullSecret GHCR (`ghcr-secret`) depuis le namespace `sf2`** (idem `allbal`) :
   ```bash
   kubectl get secret ghcr-secret -n sf2 -o json \
     | jq 'del(.metadata.namespace,.metadata.resourceVersion,.metadata.uid,.metadata.creationTimestamp)' \
     | kubectl apply -n morning-review -f -
   ```

3. **Créer le Secret BasicAuth Traefik (`morning-review-basic-auth-secret`)** (ou copier celui de `allbal`) :
   ```bash
   # Option A : Réutiliser exactement les mêmes identifiants BasicAuth que allbal
   kubectl get secret allbal-basic-auth-secret -n allbal -o json \
     | jq '.metadata.name = "morning-review-basic-auth-secret" | del(.metadata.namespace,.metadata.resourceVersion,.metadata.uid,.metadata.creationTimestamp)' \
     | kubectl apply -n morning-review -f -

   # Option B : Générer un nouveau couple utilisateur/mot de passe avec htpasswd
   htpasswd -nb mathieu 'VOTRE_MOT_DE_PASSE_SOLIDE' > /tmp/users.htpasswd
   kubectl -n morning-review create secret generic morning-review-basic-auth-secret \
     --from-file=users=/tmp/users.htpasswd \
     --dry-run=client -o yaml | kubectl apply -f -
   rm -f /tmp/users.htpasswd
   ```

4. **Créer le Secret applicatif (`morning-review-env-secret`) directement depuis le fichier `.env`** (incluant `GMAIL_ACCOUNT`, `GMAIL_PWD`, `OPENROUTER_API_KEY`, `B2_ENDPOINT_URL`, `B2_KEY_ID`, `B2_APPLICATION_KEY`, `B2_BUCKET`, etc.) :
   ```bash
   kubectl -n morning-review create secret generic morning-review-env-secret \
     --from-env-file=.env \
     --dry-run=client -o yaml | kubectl apply -f -
   ```
   > **Note** : Même si votre `.env` local contient `DATABASE_URL=postgresql+psycopg://...localhost...` ou `PUBLIC_UI_URL=http://localhost:8000`, le manifeste [`k8s/deployment.yaml`](k8s/deployment.yaml) surcharge automatiquement `DATABASE_URL` avec l'URI du cluster CloudNativePG interne (`morning-review-pg-app`) et `PUBLIC_UI_URL` avec `https://review.perso.frenchtoast.dev`.

### 5.2. Déploiement GitOps via ArgoCD

Appliquer l'Application ArgoCD :
```bash
kubectl apply -f k8s/argocd-application.yaml
```

### 5.3. Composants déployés par `k8s/deployment.yaml`

- **`Cluster` CloudNativePG (`morning-review-pg`)** : PostgreSQL 17.2 (`local-path`, `2Gi`) avec activation automatique de `CREATE EXTENSION IF NOT EXISTS vector;`.
- **`PersistentVolumeClaim` (`morning-review-cache-pvc`)** : Volume `1Gi` partageant le modèle ONNX FastEmbed (`/app/.cache/fastembed`) et les rapports Markdown (`/app/reports`) entre le serveur web et les `CronJob`.
- **`Deployment` (`morning-review`)** : Serveur FastAPI/Uvicorn + UI Web sur `https://review.perso.frenchtoast.dev`.
- **`CronJob` `morning-review-refresh-hourly` (`0 * * * *`, `Europe/Paris`)** : Relève horaire Gmail IMAP (incluant l'auto-validation prudente des inscriptions newsletters double opt-in), relève RSS/Podcasts/YouTube et recalcul du clustering.
- **`CronJob` `morning-review-daily-06h30` (`30 6 * * *`, `Europe/Paris`)** : Relève complète à **06h30**, consolidation du clustering, génération du Morning Review et envoi par e-mail de `mathieuv.alterai@gmail.com` vers `mathieu.veron@gmail.com` (avec liens publics `https://review.perso.frenchtoast.dev`).
