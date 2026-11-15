from __future__ import annotations

import argparse
import json
from uuid import UUID

from pathlib import Path

from morning_review.connectors.gmail_imap import fetch_gmail_newsletters
from morning_review.connectors.mcp_enricher import enrich_topic_content
from morning_review.connectors.rss import fetch_rss_sources
from morning_review.connectors.source_registry import (
    export_sources_to_yaml,
    list_user_sources,
    sync_sources_from_yaml,
)
from morning_review.connectors.youtube_and_podcasts import (
    import_youtube_channels_or_csv,
    list_creators_aggregated,
    sync_youtube_subscriptions_oauth,
)
from morning_review.db.session import SessionLocal, init_db
from morning_review.pipeline.ingestion import recluster_existing_topics
from morning_review.pipeline.synthesizer import corroborate_topic_via_web
from morning_review.pipeline.search_and_profile import (
    generate_daily_digest,
    record_topic_feedback,
    search_topics,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="morning-review",
        description="CLI de gestion des sources, relève Gmail/RSS, recherche hybride et génération du Morning Review",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Initialise PostgreSQL (pgvector), le user_id UUID et synchronise config/sources.yaml")

    p_sources = sub.add_parser("sources", help="Affiche ou exporte la liste maintenue des flux et newsletters")
    p_sources.add_argument("--export", action="store_true", help="Exporte la BDD vers config/sources.yaml")

    p_gmail = sub.add_parser("fetch-gmail", help="Relève les dernières Newsletters de la boîte Gmail (.env)")
    p_gmail.add_argument("--max-messages", type=int, default=100, help="Nombre max d'emails récents à inspecter")
    p_gmail.add_argument("--since-days", type=int, default=30, help="Fenêtre temporelle en jours")
    p_gmail.add_argument("--no-export", action="store_true", help="Ne pas ré-exporter config/sources.yaml")

    p_rss = sub.add_parser("fetch-rss", help="Relève les flux RSS actifs")
    p_rss.add_argument("--max-entries", type=int, default=8, help="Nombre max d'articles par flux")

    p_search = sub.add_parser("search", help="Recherche par mot-clé, sémantique ou hybride (RRF)")
    p_search.add_argument("query", type=str, help="Requête de recherche")
    p_search.add_argument("--mode", choices=["hybrid", "keyword", "semantic"], default="hybrid")
    p_search.add_argument("--category", type=str, default=None)

    p_fb = sub.add_parser("feedback", help="Auto-profilage : indique si un topic_id est pertinent (+1) ou non (-1)")
    p_fb.add_argument("topic_id", type=str, help="UUID du sujet (topic_id)")
    p_fb.add_argument("--vote", type=int, choices=[1, -1], required=True, help="1 (pertinent) ou -1 (non pertinent)")

    p_enrich = sub.add_parser(
        "enrich",
        help="Enrichit à la demande le contenu complet d'un topic_id (via agent-auth-mcp abonné ou trafilatura public)",
    )
    p_enrich.add_argument("topic_id", type=str, help="UUID du sujet (topic_id)")

    sub.add_parser(
        "cluster",
        help="Consolide et fusionne les topics liés au même événement d'actualité (Hybrid Story Clustering)",
    )

    p_corrob = sub.add_parser(
        "corroborate",
        help="Croise les sources d'un topic_id via une recherche Web d'actualités et génère une synthèse multi-sources",
    )
    p_corrob.add_argument("topic_id", type=str, help="UUID du sujet (topic_id)")

    p_digest = sub.add_parser("digest", help="Génère le rapport quotidien Morning Review (BDD + S3 + reports/) et l'envoie optionnellement par e-mail")
    p_digest.add_argument("--max-topics", type=int, default=25)
    p_digest.add_argument("--send-email", action="store_true", help="Expédie le digest par e-mail (idempotent 1 fois/jour)")
    p_digest.add_argument("--force-send", action="store_true", help="Force le renvoi par e-mail même si déjà envoyé aujourd'hui")
    p_digest.add_argument("--to", type=str, default=None, help="Destinataire e-mail optionnel (défaut: DIGEST_RECIPIENT_EMAIL)")

    sub.add_parser(
        "automations",
        help="Affiche les automations déclaratives (.agents/schedules.json) et l'état du démon / dernier envoi quotidien",
    )

    p_creators = sub.add_parser("creators", help="Affiche les créateurs de contenu agrégés multi-canaux (Podcast, YouTube, RSS, Newsletter)")
    p_creators.add_argument("--active-only", action="store_true", help="N'affiche que les créateurs avec au moins un canal actif")

    p_yt_oauth = sub.add_parser("sync-youtube", help="Synchronise les abonnements YouTube via OAuth Token (YouTube Data API v3)")
    p_yt_oauth.add_argument("--token", type=str, default=None, help="OAuth 2.0 Bearer Token (ou variable YOUTUBE_OAUTH_TOKEN dans .env)")
    p_yt_oauth.add_argument("--category", type=str, default="tech", help="Catégorie par défaut")

    p_yt_import = sub.add_parser("import-youtube", help="Importe des chaînes YouTube (@handles, URLs) ou un fichier subscriptions.csv Google Takeout")
    p_yt_import.add_argument("items", nargs="*", help="Liste de @handles, URLs ou noms de chaînes YouTube")
    p_yt_import.add_argument("--csv", type=str, default=None, help="Chemin vers subscriptions.csv (Google Takeout)")
    p_yt_import.add_argument("--category", type=str, default="tech", help="Catégorie par défaut")
    p_yt_import.add_argument("--creator", type=str, default=None, help="Nom du créateur de contenu à associer")

    p_serve = sub.add_parser("serve", help="Démarre l'interface Web légère et l'API FastAPI (mono-worker)")
    p_serve.add_argument("--host", type=str, default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8000)

    args = parser.parse_args()
    if args.command == "serve":
        import uvicorn

        uvicorn.run("morning_review.api.app:app", host=args.host, port=args.port, reload=False)
        return

    user_id = init_db()

    with SessionLocal() as session:
        if args.command == "init":
            res = sync_sources_from_yaml(session, user_id=user_id)
            print(json.dumps({"user_id": str(user_id), **res}, indent=2, ensure_ascii=False))

        elif args.command == "sources":
            if args.export:
                path = export_sources_to_yaml(session, user_id=user_id)
                print(f"Exporté vers : {path}")
            items = list_user_sources(session, user_id=user_id)
            print(json.dumps(items, indent=2, ensure_ascii=False))

        elif args.command == "fetch-gmail":
            sync_sources_from_yaml(session, user_id=user_id)
            res = fetch_gmail_newsletters(
                session,
                user_id=user_id,
                max_messages=args.max_messages,
                since_days=args.since_days,
            )
            if not args.no_export:
                export_sources_to_yaml(session, user_id=user_id)
            print(json.dumps(res, indent=2, ensure_ascii=False))

        elif args.command == "fetch-rss":
            sync_sources_from_yaml(session, user_id=user_id)
            res = fetch_rss_sources(session, user_id=user_id, max_entries_per_feed=args.max_entries)
            print(json.dumps(res, indent=2, ensure_ascii=False))

        elif args.command == "search":
            res = search_topics(
                session,
                user_id=user_id,
                query=args.query,
                mode=args.mode,
                category_slug=args.category,
            )
            print(json.dumps(res, indent=2, ensure_ascii=False))

        elif args.command == "feedback":
            res = record_topic_feedback(
                session,
                user_id=user_id,
                topic_id=UUID(args.topic_id),
                vote=args.vote,
            )
            print(json.dumps(res, indent=2, ensure_ascii=False))

        elif args.command == "enrich":
            res = enrich_topic_content(
                session,
                user_id=user_id,
                topic_id=UUID(args.topic_id),
            )
            print(json.dumps(res, indent=2, ensure_ascii=False))

        elif args.command == "cluster":
            res = recluster_existing_topics(session, user_id=user_id)
            print(json.dumps(res, indent=2, ensure_ascii=False))

        elif args.command == "corroborate":
            res = corroborate_topic_via_web(
                session,
                user_id=user_id,
                topic_id=UUID(args.topic_id),
            )
            print(json.dumps(res, indent=2, ensure_ascii=False))

        elif args.command == "digest":
            res = generate_daily_digest(
                session,
                user_id=user_id,
                max_topics=args.max_topics,
                send_email=args.send_email,
                force_send=args.force_send,
                recipient_email=args.to,
            )
            print(json.dumps(res, indent=2, ensure_ascii=False))

        elif args.command == "automations":
            import socket
            from morning_review.core.config import PROJECT_ROOT, settings
            from morning_review.db.models import UserDigest
            from sqlalchemy import select

            sched_path = PROJECT_ROOT / ".agents" / "schedules.json"
            sched_data = (
                json.loads(sched_path.read_text(encoding="utf-8"))
                if sched_path.exists()
                else {"daemons": [], "schedules": []}
            )
            port_open = False
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1.0)
                port_open = sock.connect_ex(("127.0.0.1", 8000)) == 0
            latest_digest = session.scalar(
                select(UserDigest)
                .where(UserDigest.user_id == user_id)
                .order_by(UserDigest.digest_date.desc())
                .limit(1)
            )
            print(
                json.dumps(
                    {
                        "schedules_file": str(sched_path),
                        "uvicorn_port_8000_up": port_open,
                        "digest_recipient_email": settings.digest_recipient_email,
                        "latest_digest_date": latest_digest.digest_date.isoformat() if latest_digest else None,
                        "latest_digest_emailed_at": (
                            latest_digest.emailed_at.isoformat()
                            if (latest_digest and latest_digest.emailed_at)
                            else None
                        ),
                        "declared_daemons": sched_data.get("daemons", []),
                        "declared_schedules": sched_data.get("schedules", []),
                    },
                    indent=2,
                    ensure_ascii=False,
                )
            )

        elif args.command == "creators":
            res = list_creators_aggregated(
                session,
                user_id=user_id,
                only_creator_media=True,
            )
            if args.active_only:
                res = [c for c in res if c.get("enabled_count", 0) > 0]
            print(json.dumps(res, indent=2, ensure_ascii=False))

        elif args.command == "sync-youtube":
            res = sync_youtube_subscriptions_oauth(
                session,
                user_id=user_id,
                oauth_token=args.token,
                default_category=args.category,
            )
            print(json.dumps(res, indent=2, ensure_ascii=False))

        elif args.command == "import-youtube":
            raw_input = (
                Path(args.csv).read_text(encoding="utf-8")
                if args.csv
                else "\n".join(args.items or [])
            )
            res = import_youtube_channels_or_csv(
                session,
                user_id=user_id,
                raw_input=raw_input,
                category_slug=args.category,
                creator=args.creator,
            )
            print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
