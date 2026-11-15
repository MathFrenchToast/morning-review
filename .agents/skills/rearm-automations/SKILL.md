---
name: rearm-automations
description: >-
  Réarme en une seule commande l'ensemble des démons et crons déclaratifs du projet
  stockés dans `.agents/schedules.json` (après un redémarrage machine, une reprise
  de session ou à la demande via `rearm`).
---

# Réarmement Express des Automations (`.agents/schedules.json`)

Activer cette skill dès que l'utilisateur demande de relancer les automations/crons (ex: commande courte `rearm`) ou après un redémarrage système / reprise de session.

## Procédure en 4 Étapes

1. **Inspecter l'état courant** :
   - Appeler `manage_task(Action="list")` pour lister les tâches démon et `schedule` déjà actives dans la conversation.
   - Exécuter `cd /home/mathieu/dev/projects/perso/morning-review/backend && uv run morning-review automations` pour lire `.agents/schedules.json`, l'état du port `8000` et la date du dernier envoi e-mail.
2. **Démarrer l'infrastructure Docker si nécessaire** :
   - `cd /home/mathieu/dev/projects/perso/morning-review && docker compose up -d`
3. **Réarmer les démons manquants (`daemons` dans `.agents/schedules.json`)** :
   - Si `uvicorn_port_8000_up` vaut `false`, lancer `cd /home/mathieu/dev/projects/perso/morning-review/backend && uv run morning-review serve --port 8000` avec `IsDaemon=True`.
4. **Réarmer les crons manquants (`schedules` dans `.agents/schedules.json`)** :
   - Pour chaque entrée `"enabled": true` absente de `manage_task(Action="list")`, appeler l'outil `schedule` avec `CronExpression`, `ExactTiming`, `IsDaemon=True` et `Prompt` tels que définis dans `.agents/schedules.json`.
