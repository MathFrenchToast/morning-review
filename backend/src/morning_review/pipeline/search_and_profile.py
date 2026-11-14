from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import numpy as np
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from morning_review.core.config import PROJECT_ROOT
from morning_review.core.storage import storage
from morning_review.db.models import (
    CanonicalItem,
    Source,
    SourceMention,
    Topic,
    UserDigest,
    UserProfile,
    UserTopicFeedback,
    UserTopicState,
    utcnow,
)
from morning_review.pipeline.embeddings import embedding_service


def parse_date_filter_bounds(
    period: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[datetime | None, datetime | None]:
    """Calcule les bornes UTC `(start_dt, end_dt)` à partir d'une période rapide (`24h`, `48h`, `7d`, `30d`)
    et/ou de dates explicites `date_from` / `date_to` (`YYYY-MM-DD` ou ISO-8601).
    """
    now = utcnow()
    start_dt: datetime | None = None
    end_dt: datetime | None = None

    p_clean = (period or "").strip().lower()
    period_deltas = {
        "24h": timedelta(hours=24),
        "48h": timedelta(hours=48),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30),
    }
    if p_clean in period_deltas:
        start_dt = now - period_deltas[p_clean]

    if date_from and date_from.strip():
        raw_from = date_from.strip()
        try:
            if len(raw_from) == 10:
                d_from = date.fromisoformat(raw_from)
                start_dt = datetime(d_from.year, d_from.month, d_from.day, 0, 0, 0, tzinfo=timezone.utc)
            else:
                dt = datetime.fromisoformat(raw_from.replace("Z", "+00:00"))
                start_dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    if date_to and date_to.strip():
        raw_to = date_to.strip()
        try:
            if len(raw_to) == 10:
                d_to = date.fromisoformat(raw_to)
                end_dt = datetime(d_to.year, d_to.month, d_to.day, 23, 59, 59, 999999, tzinfo=timezone.utc)
            else:
                dt = datetime.fromisoformat(raw_to.replace("Z", "+00:00"))
                end_dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    return start_dt, end_dt


def search_topics(
    session: Session,
    user_id: UUID,
    query: str,
    *,
    mode: str = "hybrid",
    category_slug: str | None = None,
    archived_mode: str = "exclude",
    date_field: str = "last_seen_at",
    period: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    sort_by: str = "score",
    sort_order: str = "desc",
    limit: int = 15,
) -> list[dict[str, Any]]:
    """Recherche dans les sujets par mot-clé (tsvector GIN), sémantique (pgvector HNSW) ou hybride (RRF k=60),
    avec filtrage et tri possibles sur `first_seen_at` (1re source), `last_seen_at` (dernière source) et `archived_mode`.
    """
    q_clean = (query or "").strip()
    if not q_clean:
        return []

    visibility_filter = or_(Topic.visibility == "public", Topic.user_id == user_id)
    archived_subq = select(UserTopicState.topic_id).where(
        UserTopicState.user_id == user_id,
        UserTopicState.is_archived.is_(True),
    )
    cats = (
        [c.strip().lower() for c in category_slug.split(",") if c.strip()]
        if category_slug
        else []
    )
    thematic_cats = [c for c in cats if c not in ("podcast", "youtube")]
    media_labels = [c for c in cats if c in ("podcast", "youtube")]
    media_topic_ids = (
        select(CanonicalItem.topic_id)
        .join(SourceMention, SourceMention.item_id == CanonicalItem.id)
        .join(Source, Source.id == SourceMention.source_id)
        .where(Source.kind.in_(media_labels))
        if media_labels
        else None
    )
    target_date_col = (
        Topic.first_seen_at
        if date_field in ("first_seen_at", "first_source")
        else Topic.last_seen_at
    )
    start_dt, end_dt = parse_date_filter_bounds(period=period, date_from=date_from, date_to=date_to)

    lexical_ranks: dict[UUID, int] = {}
    semantic_ranks: dict[UUID, int] = {}
    topics_by_id: dict[UUID, Topic] = {}
    sim_by_id: dict[UUID, float] = {}

    if mode in ("hybrid", "keyword"):
        ts_query = func.websearch_to_tsquery("simple", q_clean)
        rank_expr = func.ts_rank_cd(Topic.fts, ts_query)
        stmt = (
            select(Topic, rank_expr.label("lex_score"))
            .where(visibility_filter, Topic.fts.op("@@")(ts_query))
            .order_by(rank_expr.desc(), Topic.last_seen_at.desc())
            .limit(limit * 2)
        )
        if archived_mode == "only":
            stmt = stmt.where(Topic.id.in_(archived_subq))
        elif archived_mode == "exclude":
            stmt = stmt.where(Topic.id.not_in(archived_subq))
        if thematic_cats:
            stmt = stmt.where(Topic.category_slug.in_(thematic_cats))
        if media_topic_ids is not None:
            stmt = stmt.where(Topic.id.in_(media_topic_ids))
        if start_dt is not None:
            stmt = stmt.where(target_date_col >= start_dt)
        if end_dt is not None:
            stmt = stmt.where(target_date_col <= end_dt)
        for rank_idx, row in enumerate(session.execute(stmt).all(), start=1):
            t: Topic = row[0]
            topics_by_id[t.id] = t
            lexical_ranks[t.id] = rank_idx

    if mode in ("hybrid", "semantic"):
        q_vec = embedding_service.embed_one(q_clean)
        dist_expr = Topic.centroid_embedding.cosine_distance(q_vec)
        stmt = (
            select(Topic, (1.0 - dist_expr).label("similarity"))
            .where(visibility_filter, Topic.centroid_embedding.is_not(None))
            .order_by(dist_expr.asc())
            .limit(limit * 2)
        )
        if archived_mode == "only":
            stmt = stmt.where(Topic.id.in_(archived_subq))
        elif archived_mode == "exclude":
            stmt = stmt.where(Topic.id.not_in(archived_subq))
        if thematic_cats:
            stmt = stmt.where(Topic.category_slug.in_(thematic_cats))
        if media_topic_ids is not None:
            stmt = stmt.where(Topic.id.in_(media_topic_ids))
        if start_dt is not None:
            stmt = stmt.where(target_date_col >= start_dt)
        if end_dt is not None:
            stmt = stmt.where(target_date_col <= end_dt)
        for rank_idx, row in enumerate(session.execute(stmt).all(), start=1):
            t = row[0]
            topics_by_id[t.id] = t
            semantic_ranks[t.id] = rank_idx
            sim_by_id[t.id] = round(float(row[1] or 0.0), 4)

    scored: list[tuple[float, Topic]] = []
    k = 60.0
    for tid, topic in topics_by_id.items():
        if mode == "keyword":
            score = 1.0 / (k + lexical_ranks[tid])
        elif mode == "semantic":
            score = sim_by_id.get(tid, 0.0)
        else:
            s_lex = (1.0 / (k + lexical_ranks[tid])) if tid in lexical_ranks else 0.0
            s_sem = (1.0 / (k + semantic_ranks[tid])) if tid in semantic_ranks else 0.0
            score = s_lex + s_sem
        scored.append((score, topic))

    rev = (sort_order or "desc").lower() != "asc"
    if sort_by in ("first_seen_at", "first_source"):
        scored.sort(key=lambda pair: (pair[1].first_seen_at or utcnow(), pair[0]), reverse=rev)
    elif sort_by in ("last_seen_at", "latest_source"):
        scored.sort(key=lambda pair: (pair[1].last_seen_at or utcnow(), pair[0]), reverse=rev)
    elif sort_by == "sources":
        scored.sort(key=lambda pair: (pair[1].source_count or 1, pair[1].last_seen_at or utcnow()), reverse=rev)
    else:
        scored.sort(key=lambda pair: pair[0], reverse=rev)

    from morning_review.pipeline.synthesizer import get_deduplicated_topic_articles

    state_map: dict[UUID, UserTopicState] = {
        st.topic_id: st
        for st in session.scalars(
            select(UserTopicState).where(UserTopicState.user_id == user_id)
        ).all()
    }
    feedback_map: dict[UUID, int] = {
        fb.topic_id: fb.vote
        for fb in session.scalars(
            select(UserTopicFeedback).where(UserTopicFeedback.user_id == user_id)
        ).all()
    }

    results: list[dict[str, Any]] = []
    for score, t in scored[:limit]:
        dedup_articles = get_deduplicated_topic_articles(session, t.id)
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
        results.append(
            {
                "topic_id": str(t.id),
                "title": t.title,
                "category": t.category_slug,
                "labels": topic_labels,
                "creators": topic_creators,
                "summary": t.summary,
                "primary_url": t.primary_url,
                "source_count": len(dedup_articles) or t.source_count,
                "user_vote": feedback_map.get(t.id, 0),
                "is_archived": bool(st.is_archived) if st else False,
                "archived_at": st.archived_at.isoformat() if (st and st.archived_at) else None,
                "has_updates": bool(st.has_updates) if st else False,
                "updates_summary": st.updates_summary if st else None,
                "new_articles_count": (st.new_articles_count or 0) if st else 0,
                "score": round(score, 5),
                "semantic_similarity": sim_by_id.get(t.id),
                "matched_keyword": t.id in lexical_ranks,
                "first_seen_at": first_iso,
                "last_seen_at": last_iso,
                "first_source_at": first_iso,
                "latest_source_at": last_iso,
                "articles": articles,
            }
        )
    return results


def record_topic_feedback(
    session: Session,
    user_id: UUID,
    topic_id: UUID,
    vote: int,
) -> dict[str, Any]:
    """Enregistre l'avis Pertinent (+1) / Pas pertinent (-1) sur un topic_id et met à jour
    le vecteur centroïde d'intérêt de l'utilisateur (algorithme de Rocchio).
    """
    if vote not in (1, -1):
        raise ValueError("Le vote doit valoir +1 (pertinent) ou -1 (non pertinent).")

    topic = session.get(Topic, topic_id)
    if topic is None:
        raise ValueError(f"Topic introuvable: {topic_id}")

    fb = session.get(UserTopicFeedback, {"user_id": user_id, "topic_id": topic_id})
    if fb is None:
        fb = UserTopicFeedback(user_id=user_id, topic_id=topic_id, vote=vote, voted_at=utcnow())
        session.add(fb)
    else:
        fb.vote = vote
        fb.voted_at = utcnow()

    profile = session.get(UserProfile, user_id)
    if profile is None:
        profile = UserProfile(user_id=user_id)
        session.add(profile)
        session.flush()

    if topic.centroid_embedding is not None:
        t_vec = np.asarray(list(topic.centroid_embedding), dtype=np.float32)
        if profile.interest_embedding is None:
            new_vec = t_vec if vote == 1 else (-0.25 * t_vec)
        else:
            u_vec = np.asarray(list(profile.interest_embedding), dtype=np.float32)
            weight = 0.35 if vote == 1 else -0.20
            new_vec = u_vec + (weight * t_vec)
        norm = float(np.linalg.norm(new_vec)) or 1.0
        profile.interest_embedding = (new_vec / norm).astype(float).tolist()

    cat_aff = dict(profile.category_affinities or {})
    cat_aff[topic.category_slug] = round(cat_aff.get(topic.category_slug, 0.0) + (0.2 * vote), 3)
    profile.category_affinities = cat_aff

    if vote == 1:
        profile.positive_votes = (profile.positive_votes or 0) + 1
    else:
        profile.negative_votes = (profile.negative_votes or 0) + 1
    profile.updated_at = utcnow()

    session.commit()
    return {
        "user_id": str(user_id),
        "topic_id": str(topic_id),
        "vote": vote,
        "category": topic.category_slug,
        "positive_votes": profile.positive_votes,
        "negative_votes": profile.negative_votes,
        "category_affinities": profile.category_affinities,
    }


def compute_topic_relevance(topic: Topic, profile: UserProfile | None, vote: int | None = None) -> float:
    if vote == -1:
        return -1.0
    score = 0.5
    if profile is not None:
        if profile.interest_embedding is not None and topic.centroid_embedding is not None:
            u = np.asarray(list(profile.interest_embedding), dtype=np.float32)
            v = np.asarray(list(topic.centroid_embedding), dtype=np.float32)
            cos_sim = float(np.dot(u, v) / ((np.linalg.norm(u) * np.linalg.norm(v)) or 1.0))
            score += 0.35 * cos_sim
        cat_bonus = float((profile.category_affinities or {}).get(topic.category_slug, 0.0))
        score += max(-0.2, min(0.25, cat_bonus * 0.15))

    # Bonus de corroboration multi-sources
    score += min(0.25, 0.12 * math.log1p(max(0, (topic.source_count or 1) - 1)))
    if vote == 1:
        score += 0.30
    return round(score, 4)


def generate_daily_digest(
    session: Session,
    user_id: UUID,
    *,
    digest_date: date | None = None,
    max_topics: int = 25,
) -> dict[str, Any]:
    """Génère le rapport quotidien Morning Review (persisté en BDD, sur S3 et dans reports/)."""
    target_date = digest_date or utcnow().date()
    profile = session.get(UserProfile, user_id)

    feedback_map: dict[UUID, int] = {
        fb.topic_id: fb.vote
        for fb in session.scalars(
            select(UserTopicFeedback).where(UserTopicFeedback.user_id == user_id)
        ).all()
    }

    topics = session.scalars(
        select(Topic)
        .where(or_(Topic.visibility == "public", Topic.user_id == user_id))
        .order_by(Topic.last_seen_at.desc())
        .limit(300)
    ).all()

    scored_by_cat: dict[str, list[tuple[float, Topic]]] = defaultdict(list)
    for t in topics:
        vote = feedback_map.get(t.id)
        rel = compute_topic_relevance(t, profile, vote)
        if rel < 0:
            continue
        scored_by_cat[t.category_slug].append((rel, t))

    for cat_list in scored_by_cat.values():
        cat_list.sort(key=lambda pair: (pair[0], pair[1].last_seen_at), reverse=True)

    # Garantit une diversité par catégorie (jusqu'à 8 sujets par catégorie active), puis complète au score
    selected: list[tuple[float, Topic]] = []
    seen_ids: set[UUID] = set()
    quota_per_cat = max(4, min(8, max_topics // max(1, len(scored_by_cat))))
    for cat_slug in sorted(scored_by_cat.keys()):
        for pair in scored_by_cat[cat_slug][:quota_per_cat]:
            if len(selected) < max_topics:
                selected.append(pair)
                seen_ids.add(pair[1].id)

    remaining = [
        pair
        for cat_list in scored_by_cat.values()
        for pair in cat_list
        if pair[1].id not in seen_ids
    ]
    remaining.sort(key=lambda pair: (pair[0], pair[1].last_seen_at), reverse=True)
    for pair in remaining:
        if len(selected) >= max_topics:
            break
        selected.append(pair)

    by_category: dict[str, list[dict[str, Any]]] = defaultdict(list)
    digest_topics_payload: list[dict[str, Any]] = []

    from morning_review.pipeline.synthesizer import get_deduplicated_topic_articles

    for rel_score, t in selected:
        dedup_articles = get_deduplicated_topic_articles(session, t.id)
        sources_list = [
            {
                "source_title": a["source_title"],
                "kind": a["kind"],
                "url": a["url"],
                "article_title": a["title"],
                "published_at": a.get("published_at"),
            }
            for a in dedup_articles[:10]
        ]
        first_iso = t.first_seen_at.isoformat() if t.first_seen_at else None
        last_iso = t.last_seen_at.isoformat() if t.last_seen_at else None
        entry = {
            "topic_id": str(t.id),
            "title": t.title,
            "category": t.category_slug,
            "relevance_score": rel_score,
            "source_count": t.source_count,
            "primary_url": t.primary_url,
            "first_seen_at": first_iso,
            "last_seen_at": last_iso,
            "first_source_at": first_iso,
            "latest_source_at": last_iso,
            "summary": (t.summary or "")[:360],
            "sources": sources_list,
        }
        by_category[t.category_slug].append(entry)
        digest_topics_payload.append(entry)

    md_lines = [
        "---",
        f'date: "{target_date.isoformat()}"',
        f'user_id: "{user_id}"',
        f"topics_count: {len(digest_topics_payload)}",
        "---",
        "",
        f"# Morning Review — {target_date.isoformat()}",
        "",
        f"> **Utilisateur (`user_id`)** : `{user_id}` | **Sujets sélectionnés** : **{len(digest_topics_payload)}**",
        "",
    ]

    for cat_slug, items in sorted(by_category.items()):
        md_lines.append(f"## Catégorie : `{cat_slug}` ({len(items)} sujets)")
        md_lines.append("")
        for item in items:
            md_lines.append(
                f"### [{item['title']}]({item['primary_url']}) "
                f"*(score: {item['relevance_score']} | {item['source_count']} source(s))*"
            )
            md_lines.append(
                f"- **`topic_id`** : `{item['topic_id']}` | "
                f"**1re source** : `{item['first_seen_at'] or 'n/a'}` | "
                f"**Dernière source** : `{item['last_seen_at'] or 'n/a'}`"
            )
            if item["summary"]:
                md_lines.append(f"- **Synthèse** : {item['summary']}")
            if item["sources"]:
                src_badges = ", ".join(
                    f"{s['source_title']} (`{s['kind']}`)" for s in item["sources"]
                )
                md_lines.append(f"- **Sources agrégées ({len(item['sources'])})** : {src_badges}")
                if len(item["sources"]) > 1:
                    for s in item["sources"]:
                        md_lines.append(
                            f"  - [{s['article_title']}]({s['url']}) — *{s['source_title']}*"
                        )
            md_lines.append("")

    markdown_report = "\n".join(md_lines)
    s3_key = storage.build_digest_key(user_id, target_date)
    storage.put_markdown(s3_key, markdown_report)

    reports_dir = PROJECT_ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    local_report_path = reports_dir / f"{target_date.isoformat()}-morning-review.md"
    local_report_path.write_text(markdown_report, encoding="utf-8")

    digest_row = session.scalar(
        select(UserDigest).where(
            UserDigest.user_id == user_id,
            UserDigest.digest_date == target_date,
        )
    )
    if digest_row is None:
        digest_row = UserDigest(
            user_id=user_id,
            digest_date=target_date,
            s3_key=s3_key,
            topics_json=digest_topics_payload,
        )
        session.add(digest_row)
    else:
        digest_row.s3_key = s3_key
        digest_row.topics_json = digest_topics_payload

    session.commit()
    return {
        "digest_id": str(digest_row.id),
        "user_id": str(user_id),
        "date": target_date.isoformat(),
        "topics_count": len(digest_topics_payload),
        "s3_key": s3_key,
        "local_report_path": str(local_report_path),
        "categories": {k: len(v) for k, v in by_category.items()},
    }
