from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from morning_review.connectors.gmail_imap import fetch_gmail_newsletters
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
    corroborate_topic_via_web,
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


@asynccontextmanager
async def lifespan(_app: FastAPI):
    user_id = init_db()
    with SessionLocal() as session:
        sync_sources_from_yaml(session, user_id=user_id)
    yield


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
    vote: int = Field(..., description="+1 pour pertinent, -1 pour non pertinent")


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


@app.post("/api/v1/sources")
def api_add_source(
    payload: SourceCreateRequest,
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    source = db.scalar(select(Source).where(Source.url == payload.url.strip()))
    if source is None:
        source = Source(
            kind=payload.kind,
            url=payload.url.strip(),
            site_url=payload.site_url,
            title=payload.title.strip(),
            is_public=payload.is_public,
        )
        db.add(source)
        db.flush()

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
    return {"source_id": str(source.id), "subscription_id": str(sub.id), "title": source.title}


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
        pattern="^(sources|last_seen_at|first_seen_at|latest_source|first_source|relevance)$",
        description="Critère de tri : sources | last_seen_at (dernière source) | first_seen_at (1re source) | relevance",
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
    else:
        stmt = stmt.order_by(
            Topic.source_count.asc() if is_asc else Topic.source_count.desc(),
            Topic.last_seen_at.asc() if is_asc else Topic.last_seen_at.desc(),
        )

    stmt = stmt.limit(limit if sort_by != "relevance" else max(limit, 120))
    topics = db.scalars(stmt).all()

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
                    "source_title": a["source_title"],
                    "kind": a.get("kind"),
                    "labels": a.get("labels") or [],
                    "creator": cr or "",
                    "published_at": a.get("published_at"),
                }
            )
        first_iso = t.first_seen_at.isoformat() if t.first_seen_at else None
        last_iso = t.last_seen_at.isoformat() if t.last_seen_at else None
        result.append(
            {
                "topic_id": str(t.id),
                "title": t.title,
                "category": t.category_slug,
                "labels": topic_labels,
                "creators": topic_creators,
                "summary": t.summary,
                "primary_url": t.primary_url,
                "source_count": len(dedup_articles) or t.source_count,
                "visibility": t.visibility,
                "user_vote": feedback_map.get(t.id, 0),
                "is_archived": bool(st.is_archived) if st else False,
                "archived_at": st.archived_at.isoformat() if (st and st.archived_at) else None,
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
        return record_topic_feedback(db, user_id=user_id, topic_id=topic_id, vote=payload.vote)
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
        "topics": digest.topics_json,
        "markdown": md_text,
    }


@app.post("/api/v1/digests/generate")
def api_generate_digest(
    max_topics: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    user_id: UUID = Depends(get_current_user_id),
) -> dict[str, Any]:
    return generate_daily_digest(db, user_id=user_id, max_topics=max_topics)


if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
