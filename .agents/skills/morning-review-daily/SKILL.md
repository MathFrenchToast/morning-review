---
name: morning-review-daily
description: >-
  Workflow quotidien de 07h00 pour Morning Review : vérification Docker/Uvicorn,
  relève Gmail IMAP + validation prudente des inscriptions newsletters (double opt-in),
  relève RSS/Podcasts/YouTube, enrichissement abonné (agent-auth-mcp), génération du
  digest et envoi par e-mail de mathieuv.alterai@gmail.com vers mathieu.veron@gmail.com.
---

# Workflow Quotidien Morning Review (07h00 + Envoi E-mail)

Cette skill définit la procédure exécutée chaque matin à **07h00** (ou à la demande) pour collecter, enrichir, synthétiser et expédier la revue matinale par e-mail de `mathieuv.alterai@gmail.com` vers `mathieu.veron@gmail.com`.

## Étapes du Workflow

### 1. Santé de l'Infrastructure & du Serveur
1. Vérifier que les conteneurs `morning-review-pg` et `morning-review-s3` tournent (`docker compose up -d` si nécessaire).
2. Vérifier que le serveur FastAPI/Uvicorn répond sur `http://127.0.0.1:8000/api/v1/topics?limit=1` ; s'il est arrêté, le démarrer en tâche démon (`IsDaemon=True`) :
   ```bash
   cd /home/mathieu/dev/projects/perso/morning-review/backend && uv run morning-review serve --port 8000
   ```

### 2. Relève Gmail IMAP, Confirmations Double Opt-In & Flux RSS
Exécuter les collectes incrémentales :
```bash
cd /home/mathieu/dev/projects/perso/morning-review/backend
uv run morning-review fetch-gmail --max-messages 60 --since-days 7
uv run morning-review fetch-rss --max-entries 8
```
- `fetch-gmail` détecte et valide automatiquement les liens de confirmation d'inscription newsletter (`GET` direct, redirection Ghost `?action=signup&success=true`, ou formulaire `POST` simple RFC 9110), tout en basculant en `manual_required` (bouton 1-clic dans l'e-mail et l'UI) si un CAPTCHA/défi anti-bot est présent, sans jamais tenter de le contourner.

### 3. Enrichissement Abonné Optionnel & Consolidation des Sujets
- Si des jetons éphémères sont disponibles dans `/run/user/$UID/agent-tokens/` (`pourlascience.token`, `medium.token`, `newscientist.token`), enrichir les nouveaux sujets correspondants.
- Consolider le regroupement multi-sources :
  ```bash
  cd /home/mathieu/dev/projects/perso/morning-review/backend && uv run morning-review cluster
  ```

### 4. Génération & Envoi E-mail Idempotent (07h00)
Générer le rapport quotidien (BDD + S3 + `reports/YYYY-MM-DD-morning-review.md`) et l'envoyer par `SMTP_SSL` (`smtp.gmail.com:465`) de `mathieuv.alterai@gmail.com` vers `mathieu.veron@gmail.com` :
```bash
cd /home/mathieu/dev/projects/perso/morning-review/backend && uv run morning-review digest --max-topics 25 --send-email
```
- L'envoi est **idempotent** via `user_digests.emailed_at` (`email_status: "sent"` au premier appel du jour, `"already_sent"` ensuite sauf `--force-send`).
