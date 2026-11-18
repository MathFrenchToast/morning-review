from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
import logging
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from morning_review.connectors.gmail_imap import (
    dismiss_newsletter_confirmation,
    fetch_gmail_newsletters,
    list_newsletter_confirmations,
)
from morning_review.connectors.mcp_enricher import enrich_topic_content
from morning_review.connectors.rss import fetch_rss_sources
from morning_review.connectors.source_registry import (
    export_sources_to_yaml,
    infer_source_labels_and_creator,
    list_user_sources,
    sync_sources_from_yaml,
)
from morning_review.connectors.youtube_and_podcasts import (
    _slugify_creator,
    import_youtube_channels_or_csv,
    list_creators_aggregated,
    resolve_and_add_podcast,
    sync_youtube_subscriptions_oauth,
    upsert_creator_channel_source,
)
from morning_review.core.config import PROJECT_ROOT, settings
from morning_review.core.storage import storage
from morning_review.db.models import (
    CanonicalItem,
    Source,
    SourceMention,
    Topic,
    UserCategory,
    UserDigest,
    UserProfile,
    UserSubscription,
    UserTopicFeedback,
    UserTopicState,
)
from morning_review.db.session import SessionLocal, get_db, init_db
from morning_review.pipeline.ingestion import recluster_existing_topics
from morning_review.pipeline.synthesizer import (
    build_topic_deep_detail,
    corroborate_topic_via_web,
    extract_topic_subtopics,
    get_deduplicated_topic_articles,
    set_topic_archive_state,
)
from morning_review.pipeline.search_and_profile import (
    compute_topic_relevance,
    generate_daily_digest,
    parse_date_filter_bounds,
    record_topic_feedback,
    search_topics,
)

FRONTEND_DIR = PROJECT_ROOT / "frontend"


def get_current_user_id() -> UUID:
    """En MVP (AUTH_MODE=single_user), retourne le BOOTSTRAP_USER_ID (UUID non auto-incrémenté).
    En Post-MVP (AUTH_MODE=oidc), résoudra le user_id depuis le jeton OIDC validé par JWKS.
    """
    return settings.bootstrap_user_id


logger = logging.getLogger(__name__)


def _run_periodic_ingest_sync(user_id: UUID) -> None:
    """Exécute en tâche de fond une relève incrémentale Gmail + RSS si la dernière relève date de > 25 min."""
    try:
        with SessionLocal() as session:
            sync_sources_from_yaml(session, user_id=user_id)
            latest_fetch = session.scalar(select(func.max(Source.last_fetched_at)))
            now = datetime.now(timezone.utc)
            if latest_fetch is not None:
                if latest_fetch.tzinfo is None:
                    latest_fetch = latest_fetch.replace(tzinfo=timezone.utc)
                if (now - latest_fetch) < timedelta(minutes=25):
                    return
            if settings.gmail_account and settings.gmail_pwd:
                try:
                    fetch_gmail_newsletters(session, user_id=user_id, max_messages=25, since_days=7)
                    export_sources_to_yaml(session, user_id=user_id)
                except Exception as exc:
                    logger.warning("Auto-poll Gmail warning: %s", exc)
            try:
                fetch_rss_sources(session, user_id=user_id, max_entries_per_feed=5)
            except Exception as exc:
                logger.warning("Auto-poll RSS warning: %s", exc)

            # Filet de sécurité autonome : après DAILY_DIGEST_SEND_HOUR (07h00 local), si le digest du jour
            # n'a pas encore été expédié par e-mail, le démon le génère et l'envoie automatiquement (1 seule fois/jour).
            local_now = datetime.now().astimezone()
            if (
                local_now.hour >= settings.daily_digest_send_hour
                and settings.gmail_account
                and settings.gmail_pwd
                and settings.digest_recipient_email
            ):
                today_digest = session.scalar(
                    select(UserDigest).where(
                        UserDigest.user_id == user_id,
                        UserDigest.digest_date == now.date(),
                    )
                )
                if today_digest is None or today_digest.emailed_at is None:
                    try:
                        generate_daily_digest(session, user_id=user_id, max_topics=25, send_email=True)
                    except Exception as exc:
                        logger.warning("Auto-email daily digest warning: %s", exc)
    except Exception as exc:
        logger.warning("Periodic ingest loop error: %s", exc)


async def _periodic_ingest_loop(user_id: UUID, interval_seconds: int = 1800) -> None:
    while True:
        await asyncio.to_thread(_run_periodic_ingest_sync, user_id)
        await asyncio.sleep(interval_seconds)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    user_id = init_db()
    with SessionLocal() as session:
        sync_sources_from_yaml(session, user_id=user_id)
    bg_task = (
        asyncio.create_task(_periodic_ingest_loop(user_id, interval_seconds=1800))
        if settings.enable_background_ingest
        else None
    )
    try:
        yield
    finally:
        if bg_task is not None:
            bg_task.cancel()
            with suppress(asyncio.CancelledError):
                await bg_task


app = FastAPI(
    title="Morning Review API",
    version="0.1.0",
    lifespan=lifespan,
)


class SourceCreateRequest(BaseModel):
    kind: str = Field(default="rss", description="rss | web | newsletter | mcp_auth | podcast | youtube")
    title: str
    url: str
    site_url: str | None = None
    category: str = Field(default="actu")
    creator: str | None = None
    labels: list[str] | None = None
    is_public: bool = True
    enabled: bool = True


class SourcePatchRequest(BaseModel):
    category: str | None = None
    enabled: bool | None = None
    title: str | None = None
    creator: str | None = None
    labels: list[str] | None = None


class YouTubeOAuthSyncRequest(BaseModel):
    oauth_token: str | None = Field(
        default=None,
        description="Token OAuth 2.0 Bearer (scope youtube.readonly) ou utilise YOUTUBE_OAUTH_TOKEN depuis .env",
    )
    default_category: str = Field(default="tech")
    enabled: bool = True


class YouTubeImportRequest(BaseModel):
    raw_input: str | None = Field(
        default=None,
        description="Contenu CSV Google Takeout ou liste de @handles / URLs / noms de chaînes YouTube",
    )
    csv_text: str | None = None
    channels: list[str] | None = None
    category: str = Field(default="tech")
    creator: str | None = None
    creator_override: str | None = None
    enabled: bool = True


class PodcastAddRequest(BaseModel):
    query_or_url: str | None = Field(default=None, description="Nom du podcast (résolu via iTunes API) ou URL RSS directe")
    query_or_feed_url: str | None = None
    category: str = Field(default="tech")
    creator: str | None = None
    creator_name: str | None = None
    enabled: bool = True


class CreatorToggleRequest(BaseModel):
    enabled: bool


class FeedbackRequest(BaseModel):
    vote: int = Field(..., description="+1 pour pertinent, 0 pour neutre (retirer le vote), -1 pour non pertinent")


class TopicArchiveRequest(BaseModel):
    archived: bool = Field(default=True, description="True pour archiver (mark as read), False pour désarchiver")


class TopicBatchArchiveRequest(BaseModel):
    topic_ids: list[UUID] = Field(default_factory=list)
    archived: bool = Field(default=True)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "auth_mode": settings.auth_mode}


@app.get("/api/v1/profile")
def api_get_profile(
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    profile = db.get(UserProfile, user_id)
    cats = db.scalars(
        select(UserCategory)
        .where(UserCategory.user_id == user_id)
        .order_by(UserCategory.slug.asc())
    ).all()
    total_topics = int(
        db.scalar(
            select(func.count(Topic.id)).where(
                or_(Topic.visibility == "public", Topic.user_id == user_id)
            )
        )
        or 0
    )
    archived_count = int(
        db.scalar(
            select(func.count(UserTopicState.topic_id)).where(
                UserTopicState.user_id == user_id,
                UserTopicState.is_archived.is_(True),
            )
        )
        or 0
    )
    reactivated_count = int(
        db.scalar(
            select(func.count(UserTopicState.topic_id)).where(
                UserTopicState.user_id == user_id,
                UserTopicState.is_archived.is_(False),
                UserTopicState.has_updates.is_(True),
            )
        )
        or 0
    )
    return {
        "user_id": str(user_id),
        "positive_votes": profile.positive_votes if profile else 0,
        "negative_votes": profile.negative_votes if profile else 0,
        "inbox_topics_count": max(0, total_topics - archived_count),
        "archived_topics_count": archived_count,
        "reactivated_topics_count": reactivated_count,
        "category_affinities": (profile.category_affinities if profile else {}) or {},
        "categories": [
            {"slug": c.slug, "name": c.name, "description": c.description}
            for c in cats
        ],
    }


@app.get("/api/v1/sources")
def api_list_sources(
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> list[dict[str, Any]]:
    return list_user_sources(db, user_id)


def _resolve_rss_feed_url_if_html(raw_url: str) -> str:
    """Si l'utilisateur saisit la page d'accueil HTML d'un site (ex: https://www.quantamagazine.org/),
    détecte automatiquement la balise <link rel="alternate" type="application/rss+xml"> ou teste /feed/.
    """
    import re
    from urllib.parse import urljoin

    import feedparser
    import httpx

    u = (raw_url or "").strip()
    if not u.startswith(("http://", "https://")):
        return u
    try:
        with httpx.Client(
            timeout=8.0,
            follow_redirects=True,
            headers={"User-Agent": "MorningReview/1.0 (RSS Reader; Feed Fetcher)"},
        ) as client:
            r = client.get(u)
            ctype = (r.headers.get("content-type") or "").lower()
            parsed = feedparser.parse(r.text)
            if parsed.entries or "xml" in ctype or "rss" in ctype or "atom" in ctype:
                return str(r.url)
            match = re.search(
                r'<link[^>]+type=["\']application/(?:rss|atom)\+xml["\'][^>]*href=["\']([^"\']+)["\']',
                r.text,
                flags=re.IGNORECASE,
            ) or re.search(
                r'<link[^>]+href=["\']([^"\']+)["\'][^>]*type=["\']application/(?:rss|atom)\+xml["\']',
                r.text,
                flags=re.IGNORECASE,
            )
            if match:
                return urljoin(str(r.url), match.group(1).strip())
            candidate_feed = u.rstrip("/") + "/feed/"
            r2 = client.get(candidate_feed)
            if r2.status_code == 200 and feedparser.parse(r2.text).entries:
                return str(r2.url)
    except Exception:
        pass
    return u


@app.post("/api/v1/sources")
def api_add_source(
    payload: SourceCreateRequest,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    resolved_url = (
        _resolve_rss_feed_url_if_html(payload.url.strip())
        if payload.kind in ("rss", "podcast")
        else payload.url.strip()
    )
    source = db.scalar(
        select(Source).where(or_(Source.url == resolved_url, Source.url == payload.url.strip()))
    )
    if source is None:
        source = Source(
            kind=payload.kind,
            url=resolved_url,
            site_url=payload.site_url or (payload.url.strip() if resolved_url != payload.url.strip() else None),
            title=payload.title.strip(),
            is_public=payload.is_public,
        )
        db.add(source)
        db.flush()
    elif source.url != resolved_url and (source.item_count or 0) == 0:
        source.url = resolved_url

    cat_obj = db.scalar(
        select(UserCategory).where(
            UserCategory.user_id == user_id,
            UserCategory.slug == payload.category.lower(),
        )
    )
    sub = db.scalar(
        select(UserSubscription).where(
            UserSubscription.user_id == user_id,
            UserSubscription.source_id == source.id,
        )
    )
    if sub is None:
        sub = UserSubscription(
            user_id=user_id,
            source_id=source.id,
            category_id=cat_obj.id if cat_obj else None,
            category_slug=payload.category.lower(),
            enabled=payload.enabled,
            discovery_method="api",
        )
        db.add(sub)
    else:
        sub.category_slug = payload.category.lower()
        sub.enabled = payload.enabled

    db.commit()

    ingested_now = 0
    if payload.kind in ("rss", "podcast") and resolved_url.startswith(("http://", "https://")):
        try:
            import feedparser
            import httpx
            from morning_review.connectors.rss import _extract_entry_text, _parse_entry_date
            from morning_review.pipeline.ingestion import ingest_content_item

            with httpx.Client(
                timeout=10.0,
                follow_redirects=True,
                headers={"User-Agent": "MorningReview/1.0 (RSS Reader; Feed Fetcher)"},
            ) as client:
                r = client.get(resolved_url)
                parsed = feedparser.parse(r.text)
                for entry in (parsed.entries or [])[:8]:
                    link = (getattr(entry, "link", "") or "").strip()
                    title = (getattr(entry, "title", "") or "").strip()
                    if not link or not title:
                        continue
                    summary_txt, full_txt = _extract_entry_text(entry)
                    pub_dt = _parse_entry_date(entry)
                    _, _, created = ingest_content_item(
                        db,
                        user_id=user_id,
                        source=source,
                        raw_url=link,
                        title=title,
                        markdown_body=f"# {title}\n\n{full_txt or summary_txt or title}",
                        excerpt=(summary_txt or full_txt or title)[:500],
                        published_at=pub_dt,
                        visibility="public" if source.is_public else "private",
                        category_slug=sub.category_slug or "actu",
                    )
                    if created:
                        ingested_now += 1
                db.commit()
        except Exception as exc:
            logger.warning("Immediate RSS poll on source creation failed for %s: %s", resolved_url, exc)

    return {
        "source_id": str(source.id),
        "subscription_id": str(sub.id),
        "title": source.title,
        "url": source.url,
        "ingested_now": ingested_now,
    }


@app.patch("/api/v1/sources/{source_id}")
def api_update_source(
    source_id: UUID,
    payload: SourcePatchRequest,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    source = db.get(Source, source_id)
    sub = db.scalar(
        select(UserSubscription).where(
            UserSubscription.user_id == user_id,
            UserSubscription.source_id == source_id,
        )
    )
    if source is None or sub is None:
        raise HTTPException(status_code=404, detail="Source introuvable pour cet utilisateur")

    if payload.title is not None:
        source.title = payload.title.strip()
    if payload.category is not None:
        sub.category_slug = payload.category.strip().lower()
    if payload.enabled is not None:
        sub.enabled = payload.enabled

    db.commit()
    return {"source_id": str(source.id), "category": sub.category_slug, "enabled": sub.enabled}


@app.post("/api/v1/sources/sync-yaml")
def api_sync_yaml(
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, int]:
    return sync_sources_from_yaml(db, user_id=user_id)


@app.post("/api/v1/sources/export-yaml")
def api_export_yaml(
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, str]:
    path = export_sources_to_yaml(db, user_id=user_id)
    return {"exported_to": str(path)}


@app.get("/api/v1/creators")
def api_list_creators(
    only_creator_media: bool = Query(default=True),
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> list[dict[str, Any]]:
    return list_creators_aggregated(db, user_id=user_id, only_creator_media=only_creator_media)


@app.post("/api/v1/creators/{creator_slug}/toggle")
def api_toggle_creator(
    creator_slug: str,
    payload: CreatorToggleRequest,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    rows = db.execute(
        select(Source, UserSubscription)
        .join(UserSubscription, UserSubscription.source_id == Source.id)
        .where(UserSubscription.user_id == user_id)
    ).all()
    updated = 0
    for src, sub in rows:
        _, creator = infer_source_labels_and_creator(
            kind=src.kind,
            title=src.title,
            url=src.url,
            category_slug=sub.category_slug,
            existing_meta=src.metadata_json,
        )
        if _slugify_creator(creator) == creator_slug:
            sub.enabled = payload.enabled
            updated += 1
    db.commit()
    export_sources_to_yaml(db, user_id=user_id)
    return {"creator_slug": creator_slug, "enabled": payload.enabled, "channels_updated": updated}


@app.post("/api/v1/creators/youtube/sync-oauth")
def api_sync_youtube_oauth(
    payload: YouTubeOAuthSyncRequest,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    try:
        return sync_youtube_subscriptions_oauth(
            db,
            user_id=user_id,
            oauth_token=payload.oauth_token,
            default_category=payload.default_category,
            enabled=payload.enabled,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/v1/creators/youtube/import")
def api_import_youtube(
    payload: YouTubeImportRequest,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    raw_input = (
        payload.raw_input
        or payload.csv_text
        or ("\n".join(payload.channels) if payload.channels else "")
    )
    return import_youtube_channels_or_csv(
        db,
        user_id=user_id,
        raw_input=raw_input,
        category_slug=payload.category,
        creator=payload.creator or payload.creator_override,
        enabled=payload.enabled,
    )


@app.post("/api/v1/creators/podcast/add")
def api_add_podcast(
    payload: PodcastAddRequest,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    try:
        query_val = (payload.query_or_url or payload.query_or_feed_url or "").strip()
        return resolve_and_add_podcast(
            db,
            user_id=user_id,
            query_or_url=query_val,
            category_slug=payload.category,
            creator=payload.creator or payload.creator_name,
            enabled=payload.enabled,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/v1/ingest/gmail")
def api_ingest_gmail(
    max_messages: int = Query(default=100, ge=1, le=200),
    since_days: int = Query(default=30, ge=1, le=90),
    export_yaml: bool = Query(default=True),
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    res = fetch_gmail_newsletters(
        db,
        user_id=user_id,
        max_messages=max_messages,
        since_days=since_days,
    )
    if export_yaml:
        export_sources_to_yaml(db, user_id=user_id)
    return res


@app.post("/api/v1/ingest/rss")
def api_ingest_rss(
    max_entries_per_feed: int = Query(default=8, ge=1, le=50),
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    sync_sources_from_yaml(db, user_id=user_id)
    return fetch_rss_sources(db, user_id=user_id, max_entries_per_feed=max_entries_per_feed)


@app.get("/api/v1/topics")
def api_list_topics(
    category: str | None = None,
    archived: str = Query(
        default="exclude",
        pattern="^(exclude|only|all)$",
        description="exclude (Inbox actif par défaut) | only (onglet Archives) | all",
    ),
    date_field: str = Query(
        default="last_seen_at",
        pattern="^(last_seen_at|first_seen_at|latest_source|first_source)$",
        description="Champ de date utilisé pour le filtre temporel",
    ),
    period: str | None = Query(
        default=None,
        description="Filtre rapide : 24h | 48h | 7d | 30d | all",
    ),
    date_from: str | None = Query(default=None, description="Borne de début (YYYY-MM-DD ou ISO-8601)"),
    date_to: str | None = Query(default=None, description="Borne de fin (YYYY-MM-DD ou ISO-8601)"),
    sort_by: str = Query(
        default="sources",
        pattern="^(sources|sources_all|last_seen_at|first_seen_at|latest_source|first_source|relevance)$",
        description="Critère de tri : sources (jour + sources) | sources_all (total sources) | last_seen_at | first_seen_at | relevance",
    ),
    sort_order: str = Query(default="desc", pattern="^(desc|asc)$"),
    limit: int = Query(default=40, ge=1, le=200),
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> list[dict[str, Any]]:
    profile = db.get(UserProfile, user_id)
    feedback_map: dict[UUID, int] = {
        fb.topic_id: fb.vote
        for fb in db.scalars(
            select(UserTopicFeedback).where(UserTopicFeedback.user_id == user_id)
        ).all()
    }
    state_map: dict[UUID, UserTopicState] = {
        st.topic_id: st
        for st in db.scalars(
            select(UserTopicState).where(UserTopicState.user_id == user_id)
        ).all()
    }
    archived_subq = select(UserTopicState.topic_id).where(
        UserTopicState.user_id == user_id,
        UserTopicState.is_archived.is_(True),
    )

    stmt = select(Topic).where(or_(Topic.visibility == "public", Topic.user_id == user_id))
    if archived == "only":
        stmt = stmt.where(Topic.id.in_(archived_subq))
    elif archived == "exclude":
        stmt = stmt.where(Topic.id.not_in(archived_subq))

    if category:
        cats = [c.strip().lower() for c in category.split(",") if c.strip()]
        thematic_cats = [c for c in cats if c not in ("podcast", "youtube")]
        media_labels = [c for c in cats if c in ("podcast", "youtube")]
        if thematic_cats:
            stmt = stmt.where(Topic.category_slug.in_(thematic_cats))
        if media_labels:
            media_topic_ids = (
                select(CanonicalItem.topic_id)
                .join(SourceMention, SourceMention.item_id == CanonicalItem.id)
                .join(Source, Source.id == SourceMention.source_id)
                .where(Source.kind.in_(media_labels))
            )
            stmt = stmt.where(Topic.id.in_(media_topic_ids))

    target_date_col = (
        Topic.first_seen_at
        if date_field in ("first_seen_at", "first_source")
        else Topic.last_seen_at
    )
    start_dt, end_dt = parse_date_filter_bounds(period=period, date_from=date_from, date_to=date_to)
    if start_dt is not None:
        stmt = stmt.where(target_date_col >= start_dt)
    if end_dt is not None:
        stmt = stmt.where(target_date_col <= end_dt)

    is_asc = sort_order.lower() == "asc"
    day_bucket = func.date_trunc("day", Topic.last_seen_at)
    if sort_by in ("first_seen_at", "first_source"):
        stmt = stmt.order_by(
            Topic.first_seen_at.asc() if is_asc else Topic.first_seen_at.desc(),
            Topic.last_seen_at.desc(),
        )
    elif sort_by in ("last_seen_at", "latest_source"):
        stmt = stmt.order_by(
            Topic.last_seen_at.asc() if is_asc else Topic.last_seen_at.desc(),
            Topic.source_count.desc(),
        )
    elif sort_by == "sources_all":
        stmt = stmt.order_by(
            Topic.source_count.asc() if is_asc else Topic.source_count.desc(),
            Topic.last_seen_at.asc() if is_asc else Topic.last_seen_at.desc(),
        )
    else:
        # Tri par défaut 'sources' (Fraîcheur du jour & Sources) :
        # regroupe par jour calendaire (ex: aujourd'hui 14 nov. d'abord), puis par nombre de sources décroissant !
        stmt = stmt.order_by(
            day_bucket.asc() if is_asc else day_bucket.desc(),
            Topic.source_count.asc() if is_asc else Topic.source_count.desc(),
            Topic.last_seen_at.asc() if is_asc else Topic.last_seen_at.desc(),
        )

    fetch_limit = (
        max(limit * 3, 360)
        if (not category and sort_by == "sources") or sort_by == "relevance"
        else limit
    )
    stmt = stmt.limit(fetch_limit)
    raw_topics = list(db.scalars(stmt).all())

    if not category and sort_by == "sources" and len(raw_topics) > limit:
        from collections import defaultdict

        longform_domains = (
            "quantamagazine.org",
            "pourlascience.fr",
            "newscientist.com",
            "canardpc.com",
            "courrierinternational.com",
        )

        def _editorial_rank_key(tp: Topic) -> tuple[Any, ...]:
            dt = tp.last_seen_at or tp.first_seen_at
            day_str = dt.strftime("%Y-%m-%d") if dt else "1970-01-01"
            p_url = (tp.primary_url or "").lower()
            bonus = 2 if any(d in p_url for d in longform_domains) else 0
            return (day_str, (tp.source_count or 1) + bonus, dt.isoformat() if dt else "")

        raw_topics.sort(key=_editorial_rank_key, reverse=not is_asc)
        by_cat: dict[str, list[Topic]] = defaultdict(list)
        for tp in raw_topics:
            by_cat[tp.category_slug or "actu"].append(tp)

        quota = max(15, limit // max(1, len(by_cat)))
        balanced: list[Topic] = []
        seen_tids: set[UUID] = set()
        for c_slug in ("science", "gaming-hardware", "tech", "actu", "perso"):
            for tp in by_cat.get(c_slug, [])[:quota]:
                if len(balanced) < limit and tp.id not in seen_tids:
                    balanced.append(tp)
                    seen_tids.add(tp.id)
        for tp in raw_topics:
            if len(balanced) >= limit:
                break
            if tp.id not in seen_tids:
                balanced.append(tp)
                seen_tids.add(tp.id)
        balanced.sort(key=_editorial_rank_key, reverse=not is_asc)
        topics = balanced
    else:
        topics = raw_topics

    result: list[dict[str, Any]] = []
    for t in topics:
        dedup_articles = get_deduplicated_topic_articles(db, t.id)
        st = state_map.get(t.id)
        topic_labels: list[str] = [t.category_slug] if t.category_slug else []
        topic_creators: list[str] = []
        articles = []
        for a in dedup_articles[:10]:
            for lb in a.get("labels") or []:
                if lb not in topic_labels:
                    topic_labels.append(lb)
            cr = a.get("creator")
            if cr and cr not in topic_creators:
                topic_creators.append(cr)
            articles.append(
                {
                    "title": a["title"],
                    "url": a["url"],
                    "excerpt": a.get("excerpt") or "",
                    "source_title": a["source_title"],
                    "kind": a.get("kind"),
                    "labels": a.get("labels") or [],
                    "creator": cr or "",
                    "published_at": a.get("published_at"),
                }
            )
        first_iso = t.first_seen_at.isoformat() if t.first_seen_at else None
        last_iso = t.last_seen_at.isoformat() if t.last_seen_at else None
        subtopics = extract_topic_subtopics(db, t, dedup_articles)
        result.append(
            {
                "topic_id": str(t.id),
                "title": t.title,
                "category": t.category_slug,
                "labels": topic_labels,
                "creators": topic_creators,
                "summary": t.summary,
                "subtopics": subtopics,
                "primary_url": t.primary_url,
                "source_count": len(dedup_articles) or t.source_count,
                "visibility": t.visibility,
                "user_vote": feedback_map.get(t.id, 0),
                "is_archived": bool(st.is_archived) if st else False,
                "archived_at": st.archived_at.isoformat() if (st and st.archived_at) else None,
                "reactivated_at": st.reactivated_at.isoformat() if (st and st.reactivated_at) else None,
                "has_updates": bool(st.has_updates) if st else False,
                "updates_summary": st.updates_summary if st else None,
                "new_articles_count": (st.new_articles_count or 0) if st else 0,
                "relevance_score": compute_topic_relevance(t, profile, feedback_map.get(t.id)),
                "articles": articles,
                "first_seen_at": first_iso,
                "last_seen_at": last_iso,
                "first_source_at": first_iso,
                "latest_source_at": last_iso,
            }
        )

    if sort_by == "relevance":
        result.sort(
            key=lambda item: (item["relevance_score"], item["last_seen_at"] or ""),
            reverse=not is_asc,
        )
        result = result[:limit]
    elif archived == "exclude" and not is_asc:
        # Remonte en tête les sujets réactivés (has_updates=True) pour signaler immédiatement les nouveautés
        result.sort(
            key=lambda item: (1 if item.get("has_updates") else 0),
            reverse=True,
        )

    return result


@app.post("/api/v1/topics/archive-batch")
def api_archive_topics_batch(
    payload: TopicBatchArchiveRequest,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    count = 0
    for tid in payload.topic_ids:
        try:
            set_topic_archive_state(db, user_id=user_id, topic_id=tid, archived=payload.archived)
            count += 1
        except ValueError:
            continue
    return {"updated_count": count, "archived": payload.archived}


@app.post("/api/v1/topics/{topic_id}/archive")
def api_archive_topic(
    topic_id: UUID,
    payload: TopicArchiveRequest,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    try:
        return set_topic_archive_state(
            db,
            user_id=user_id,
            topic_id=topic_id,
            archived=payload.archived,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/v1/topics/{topic_id}/detail")
def api_get_topic_detail(
    topic_id: UUID,
    auto_enrich: bool = Query(default=True, description="Enrichit automatiquement le sujet si le texte stocké est court"),
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    try:
        return build_topic_deep_detail(
            db,
            user_id=user_id,
            topic_id=topic_id,
            auto_enrich=auto_enrich,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/v1/topics/{topic_id}/content")
def api_get_topic_content(
    topic_id: UUID,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    topic = db.get(Topic, topic_id)
    if topic is None or (topic.visibility != "public" and topic.user_id != user_id):
        raise HTTPException(status_code=404, detail="Topic introuvable")

    item = db.scalar(
        select(CanonicalItem)
        .where(CanonicalItem.topic_id == topic.id)
        .order_by(CanonicalItem.created_at.desc())
    )
    markdown = storage.get_markdown(item.s3_key) if item and item.s3_key else (topic.summary or "")
    return {
        "topic_id": str(topic.id),
        "title": topic.title,
        "category": topic.category_slug,
        "primary_url": topic.primary_url,
        "s3_key": item.s3_key if item else None,
        "markdown": markdown or topic.summary or "",
    }


@app.get("/api/v1/search")
def api_search(
    q: str = Query(..., min_length=1),
    mode: str = Query(default="hybrid", pattern="^(hybrid|keyword|semantic)$"),
    category: str | None = None,
    archived: str = Query(default="exclude", pattern="^(exclude|only|all)$"),
    date_field: str = Query(
        default="last_seen_at",
        pattern="^(last_seen_at|first_seen_at|latest_source|first_source)$",
    ),
    period: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    sort_by: str = Query(
        default="score",
        pattern="^(score|sources|last_seen_at|first_seen_at|latest_source|first_source)$",
    ),
    sort_order: str = Query(default="desc", pattern="^(desc|asc)$"),
    limit: int = Query(default=20, ge=1, le=50),
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> list[dict[str, Any]]:
    return search_topics(
        db,
        user_id=user_id,
        query=q,
        mode=mode,
        category_slug=category,
        archived_mode=archived,
        date_field=date_field,
        period=period,
        date_from=date_from,
        date_to=date_to,
        sort_by=sort_by,
        sort_order=sort_order,
        limit=limit,
    )


@app.post("/api/v1/topics/{topic_id}/feedback")
def api_topic_feedback(
    topic_id: UUID,
    payload: FeedbackRequest,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    try:
        res = record_topic_feedback(db, user_id=user_id, topic_id=topic_id, vote=payload.vote)
        if payload.vote == -1:
            set_topic_archive_state(db, user_id=user_id, topic_id=topic_id, archived=True)
            res["is_archived"] = True
        return res
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/v1/topics/recluster")
def api_recluster_topics(
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    return recluster_existing_topics(db, user_id=user_id)


@app.post("/api/v1/topics/{topic_id}/corroborate")
def api_corroborate_topic(
    topic_id: UUID,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    try:
        return corroborate_topic_via_web(db, user_id=user_id, topic_id=topic_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/v1/topics/{topic_id}/enrich")
def api_enrich_topic(
    topic_id: UUID,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    try:
        return enrich_topic_content(db, user_id=user_id, topic_id=topic_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/v1/digests/latest")
def api_get_latest_digest(
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    digest = db.scalar(
        select(UserDigest)
        .where(UserDigest.user_id == user_id)
        .order_by(UserDigest.digest_date.desc())
        .limit(1)
    )
    if digest is None:
        return generate_daily_digest(db, user_id=user_id, max_topics=25)

    md_text = storage.get_markdown(digest.s3_key) or ""
    return {
        "digest_id": str(digest.id),
        "user_id": str(user_id),
        "date": digest.digest_date.isoformat(),
        "s3_key": digest.s3_key,
        "emailed_at": digest.emailed_at.isoformat() if digest.emailed_at else None,
        "emailed_to": digest.emailed_to,
        "topics": digest.topics_json,
        "markdown": md_text,
    }


@app.post("/api/v1/digests/generate")
def api_generate_digest(
    max_topics: int = Query(default=25, ge=1, le=100),
    send_email: bool = Query(default=False, description="Expédier le digest par e-mail (idempotent 1 fois/jour)"),
    force_send: bool = Query(default=False, description="Forcer le renvoi même si déjà expédié aujourd'hui"),
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    return generate_daily_digest(
        db,
        user_id=user_id,
        max_topics=max_topics,
        send_email=send_email,
        force_send=force_send,
    )


@app.get("/api/v1/newsletters/confirmations")
def api_list_newsletter_confirmations(
    include_dismissed: bool = Query(default=False),
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> list[dict[str, Any]]:
    return list_newsletter_confirmations(
        db,
        user_id=user_id,
        include_dismissed=include_dismissed,
        limit=limit,
    )


@app.post("/api/v1/newsletters/confirmations/{conf_id}/dismiss")
def api_dismiss_newsletter_confirmation(
    conf_id: UUID,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    try:
        return dismiss_newsletter_confirmation(db, user_id=user_id, conf_id=conf_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/v1/pourlascience/latest")
def api_pourlascience_latest(
    category: str = Query(default="", description="Rubrique optionnelle (ex: physique, biologie, astrophysique, mathematiques)"),
    limit: int = Query(default=20, ge=1, le=60),
) -> list[dict[str, Any]]:
    from morning_review.connectors.mcp_enricher import list_pourlascience_latest_articles

    try:
        return list_pourlascience_latest_articles(category=category, limit=limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/v1/pourlascience/issues")
def api_pourlascience_issues(
    source: str = Query(default="all", description="all | pls (mensuels) | dpls (hors-séries)"),
    year: int | None = Query(default=None, description="Année de parution (ex: 2026, 2025...)"),
    issue_number: int | None = Query(default=None, description="Numéro (ex: 589, 580, 133)"),
    q: str = Query(default="", description="Recherche dans le titre du dossier"),
    page: int = Query(default=1, ge=1, le=100),
    limit: int = Query(default=24, ge=1, le=60),
) -> list[dict[str, Any]]:
    from morning_review.connectors.mcp_enricher import list_pourlascience_issues

    try:
        return list_pourlascience_issues(
            source=source,
            year=year,
            issue_number=issue_number,
            query=q,
            page=page,
            limit=limit,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/v1/pourlascience/issues/{issue_ref}")
def api_pourlascience_issue_sommaire(
    issue_ref: str,
) -> dict[str, Any]:
    from morning_review.connectors.mcp_enricher import get_pourlascience_issue_articles

    try:
        return get_pourlascience_issue_articles(issue_ref)
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/v1/pourlascience/search")
def api_pourlascience_search(
    q: str = Query(..., min_length=2, description="Mots-clés à rechercher dans les archives Pour la Science"),
    page: int = Query(default=1, ge=1, le=50),
    limit: int = Query(default=20, ge=1, le=50),
) -> list[dict[str, Any]]:
    from morning_review.connectors.mcp_enricher import search_pourlascience_articles

    try:
        return search_pourlascience_articles(query=q, page=page, limit=limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/v1/pourlascience/article")
def api_pourlascience_read_article(
    url: str = Query(..., description="URL de l'article sur www.pourlascience.fr"),
) -> dict[str, Any]:
    from morning_review.connectors.mcp_enricher import _fetch_via_agent_auth_mcp

    try:
        return _fetch_via_agent_auth_mcp("tools.pourlascience.client:PourLaScienceClient", url)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


class EdgeEnrichmentPushRequest(BaseModel):
    topic_id: UUID
    markdown_body: str | None = None
    excerpt: str | None = None
    summary: str | None = None
    subtopics: list[dict[str, Any]] | None = None


@app.get("/api/v1/edge/pending-topics")
def api_edge_pending_topics(
    limit: int = Query(default=20, ge=1, le=60),
    user_id: UUID = Depends(get_current_user_id),
    session: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """Retourne les sujets récents sur le serveur distant qui bénéficieraient d'un enrichissement
    local Edge (sources payantes Pour la Science / New Scientist / Medium via tokens RAM locaux,
    ou sujets dont le résumé n'a pas encore été synthétisé par un LLM).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=72)
    topics = session.scalars(
        select(Topic)
        .where(
            or_(Topic.visibility == "public", Topic.user_id == user_id),
            Topic.last_seen_at >= cutoff,
        )
        .order_by(Topic.last_seen_at.desc())
        .limit(200)
    ).all()

    state_map = {
        st.topic_id: st
        for st in session.scalars(
            select(UserTopicState).where(UserTopicState.user_id == user_id)
        ).all()
    }

    pending: list[dict[str, Any]] = []
    subscriber_domains = (
        "pourlascience.fr",
        "newscientist.com",
        "medium.com",
        "lemonde.fr",
        "courrierinternational.com",
        "canardpc.com",
        "substack.com",
    )
    for t in topics:
        st = state_map.get(t.id)
        if st and st.is_archived:
            continue
        url_low = (t.primary_url or "").lower()
        is_sub_domain = any(d in url_low for d in subscriber_domains)
        primary_item = session.scalar(
            select(CanonicalItem)
            .where(CanonicalItem.topic_id == t.id)
            .order_by(CanonicalItem.published_at.desc().nullslast())
            .limit(1)
        )
        already_enriched = bool(
            primary_item and (primary_item.s3_key or "").startswith("enriched/")
        )
        needs_summary = (
            "• [" in (t.summary or "")
            or "member-only story" in (t.summary or "").lower()
            or "**genre :" in (t.summary or "").lower()
            or "tronqué dans l'extrait" in (t.summary or "").lower()
            or len(t.summary or "") < 180
        )
        if (is_sub_domain and not already_enriched) or needs_summary:
            pending.append(
                {
                    "topic_id": str(t.id),
                    "title": t.title,
                    "primary_url": t.primary_url,
                    "category": t.category_slug,
                    "is_subscriber_domain": is_sub_domain,
                    "already_enriched": already_enriched,
                    "current_summary": t.summary or "",
                    "articles": get_deduplicated_topic_articles(session, t.id),
                }
            )
            if len(pending) >= limit:
                break
    return pending


@app.post("/api/v1/edge/push-enrichment")
def api_edge_push_enrichment(
    payload: EdgeEnrichmentPushRequest,
    user_id: UUID = Depends(get_current_user_id),
    session: Session = Depends(get_db),
) -> dict[str, Any]:
    """Reçoit depuis le poste local (Edge) le contenu Markdown intégral (extrait via tokens RAM agent-auth-mcp
    ou Trafilatura) et/ou la synthèse LLM calculée localement, et met à jour la BDD et le stockage S3 distant.
    """
    from morning_review.pipeline.ingestion import clean_aggregator_noise

    topic = session.get(Topic, payload.topic_id)
    if topic is None:
        raise HTTPException(status_code=404, detail="Topic introuvable")

    primary_item = session.scalar(
        select(CanonicalItem)
        .where(CanonicalItem.topic_id == topic.id)
        .order_by(CanonicalItem.published_at.desc().nullslast())
        .limit(1)
    )
    s3_key_updated = None
    if payload.markdown_body and len(payload.markdown_body.strip()) >= 200 and primary_item is not None:
        md_clean = payload.markdown_body.strip()
        content_hash = storage.compute_sha256(md_clean)
        s3_key_updated = f"enriched/{content_hash[:2]}/{content_hash}.md"
        storage.put_markdown(s3_key_updated, md_clean)
        primary_item.s3_key = s3_key_updated
        primary_item.content_sha256 = content_hash
        if payload.excerpt:
            primary_item.excerpt = clean_aggregator_noise(payload.excerpt)[:600]

    if payload.summary and len(payload.summary.strip()) >= 60:
        topic.summary = clean_aggregator_noise(payload.summary.strip())[:1200]
    if payload.subtopics is not None:
        topic.subtopics_json = payload.subtopics

    session.commit()
    return {
        "status": "updated",
        "topic_id": str(topic.id),
        "s3_key": s3_key_updated or (primary_item.s3_key if primary_item else None),
        "summary_len": len(topic.summary or ""),
    }


if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
