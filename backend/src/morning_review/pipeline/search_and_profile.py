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

    from morning_review.pipeline.synthesizer import (
        extract_topic_subtopics,
        get_deduplicated_topic_articles,
    )

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
        subtopics = extract_topic_subtopics(session, t, dedup_articles)
        results.append(
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
                "user_vote": feedback_map.get(t.id, 0),
                "is_archived": bool(st.is_archived) if st else False,
                "archived_at": st.archived_at.isoformat() if (st and st.archived_at) else None,
                "reactivated_at": st.reactivated_at.isoformat() if (st and st.reactivated_at) else None,
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
    """Enregistre l'avis Pertinent (+1) / Neutre (0, retrait du vote) / Pas pertinent (-1)
    sur un topic_id et met à jour le vecteur centroïde d'intérêt de l'utilisateur (algorithme de Rocchio).
    """
    if vote not in (1, 0, -1):
        raise ValueError("Le vote doit valoir +1 (pertinent), 0 (neutre) ou -1 (non pertinent).")

    topic = session.get(Topic, topic_id)
    if topic is None:
        raise ValueError(f"Topic introuvable: {topic_id}")

    fb = session.get(UserTopicFeedback, {"user_id": user_id, "topic_id": topic_id})
    prev_vote = fb.vote if fb is not None else 0

    if vote == 0:
        if fb is not None:
            session.delete(fb)
    else:
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

    delta_vote = vote - prev_vote

    if topic.centroid_embedding is not None and delta_vote != 0:
        t_vec = np.asarray(list(topic.centroid_embedding), dtype=np.float32)
        if profile.interest_embedding is None:
            if vote != 0:
                new_vec = t_vec if vote == 1 else (-0.25 * t_vec)
                norm = float(np.linalg.norm(new_vec)) or 1.0
                profile.interest_embedding = (new_vec / norm).astype(float).tolist()
        else:
            u_vec = np.asarray(list(profile.interest_embedding), dtype=np.float32)
            if vote == 1:
                weight = 0.35
            elif vote == -1:
                weight = -0.20
            else:
                # Retrait d'un vote précédent (retour à 0)
                weight = -0.35 if prev_vote == 1 else 0.20
            new_vec = u_vec + (weight * t_vec)
            norm = float(np.linalg.norm(new_vec)) or 1.0
            profile.interest_embedding = (new_vec / norm).astype(float).tolist()

    cat_aff = dict(profile.category_affinities or {})
    cat_aff[topic.category_slug] = round(cat_aff.get(topic.category_slug, 0.0) + (0.2 * delta_vote), 3)
    profile.category_affinities = cat_aff

    if prev_vote == 1 and vote != 1:
        profile.positive_votes = max(0, (profile.positive_votes or 0) - 1)
    elif prev_vote == -1 and vote != -1:
        profile.negative_votes = max(0, (profile.negative_votes or 0) - 1)

    if prev_vote != 1 and vote == 1:
        profile.positive_votes = (profile.positive_votes or 0) + 1
    elif prev_vote != -1 and vote == -1:
        profile.negative_votes = (profile.negative_votes or 0) + 1

    profile.updated_at = utcnow()

    session.commit()
    return {
        "user_id": str(user_id),
        "topic_id": str(topic_id),
        "previous_vote": prev_vote,
        "vote": vote,
        "category": topic.category_slug,
        "positive_votes": profile.positive_votes,
        "negative_votes": profile.negative_votes,
        "category_affinities": profile.category_affinities,
    }


HIGH_SIGNAL_LONGFORM_DOMAINS: tuple[str, ...] = (
    "quantamagazine.org",
    "pourlascience.fr",
    "newscientist.com",
    "construction-physics.com",
    "dwarkesh.com",
    "pragmaticengineer.com",
    "bytebytego.com",
    "latent.space",
    "importai.net",
    "arstechnica.com",
    "404media.co",
    "nature.com",
    "science.org",
    "lwn.net",
    "danluu.com",
    "simonwillison.net",
    "stratechery.com",
    "ben-evans.com",
    "courrierinternational.com",
)


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

    # Bonus de corroboration multi-sources (plafonné pour ne pas écraser les articles de fond)
    score += min(0.22, 0.10 * math.log1p(max(0, min(8, (topic.source_count or 1)) - 1)))

    # Bonus d'autorité et de profondeur éditoriale (Long-form : Quanta Magazine, Pour la Science, etc.)
    primary_url_low = (topic.primary_url or "").lower()
    if any(dom in primary_url_low for dom in HIGH_SIGNAL_LONGFORM_DOMAINS):
        score += 0.24
    elif len(topic.summary or "") >= 380 and "• [" not in (topic.summary or ""):
        score += 0.10

    # Fraîcheur temporelle (Time Decay sur first_seen_at / last_seen_at)
    ref_dt = topic.first_seen_at or topic.last_seen_at
    if ref_dt is not None:
        ref_utc = ref_dt.replace(tzinfo=timezone.utc) if ref_dt.tzinfo is None else ref_dt
        age_hours = max(0.0, (utcnow() - ref_utc).total_seconds() / 3600.0)
        if age_hours <= 24.0:
            score += 0.16
        elif age_hours <= 36.0:
            score += 0.04
        elif age_hours > 48.0 and vote != 1:
            score -= min(0.35, 0.08 * ((age_hours - 48.0) / 24.0))

    if vote == 1:
        score += 0.30
    return round(score, 4)


def generate_daily_digest(
    session: Session,
    user_id: UUID,
    *,
    digest_date: date | None = None,
    max_topics: int = 12,
    recent_hours: int = 24,
    send_email: bool = False,
    force_send: bool = False,
    recipient_email: str | None = None,
    smtp_factory: Any = None,
) -> dict[str, Any]:
    """Génère le rapport quotidien Morning Review (persisté en BDD, sur S3 et dans reports/),
    avec mémoire stricte des digests précédents (un sujet déjà présenté n'est jamais ré-envoyé
    sauf rebondissement factuel réel validé postérieurement) et enrichissement + synthèse LLM
    sur le contenu intégral des articles retenus.
    """
    import os

    target_date = digest_date or utcnow().date()
    profile = session.get(UserProfile, user_id)

    feedback_map: dict[UUID, int] = {
        fb.topic_id: fb.vote
        for fb in session.scalars(
            select(UserTopicFeedback).where(UserTopicFeedback.user_id == user_id)
        ).all()
    }
    state_map: dict[UUID, UserTopicState] = {
        st.topic_id: st
        for st in session.scalars(
            select(UserTopicState).where(UserTopicState.user_id == user_id)
        ).all()
    }

    # Mémoire des digests précédents : récupère tous les topic_id déjà présentés avant target_date
    previous_digests = session.scalars(
        select(UserDigest)
        .where(
            UserDigest.user_id == user_id,
            UserDigest.digest_date < target_date,
        )
        .order_by(UserDigest.digest_date.desc())
    ).all()
    previously_presented_ids: set[UUID] = set()
    last_digest_created_at: datetime | None = None
    for pd in previous_digests:
        if last_digest_created_at is None and pd.created_at is not None:
            last_digest_created_at = (
                pd.created_at.replace(tzinfo=timezone.utc)
                if pd.created_at.tzinfo is None
                else pd.created_at
            )
        for item in pd.topics_json or []:
            raw_tid = item.get("topic_id") if isinstance(item, dict) else None
            if raw_tid:
                try:
                    previously_presented_ids.add(UUID(str(raw_tid)))
                except ValueError:
                    pass

    topics = session.scalars(
        select(Topic)
        .where(or_(Topic.visibility == "public", Topic.user_id == user_id))
        .order_by(Topic.last_seen_at.desc())
        .limit(300)
    ).all()

    unarchived_topics: list[Topic] = []
    for t in topics:
        st = state_map.get(t.id)
        if st and st.is_archived:
            continue
        if t.id in previously_presented_ids:
            # Déjà présenté dans un Morning Review précédent : exclu sauf vraie suite factuelle postérieure
            if not (st and st.has_updates and st.updates_summary):
                continue
            if last_digest_created_at is not None and st.reactivated_at is not None:
                react_utc = (
                    st.reactivated_at.replace(tzinfo=timezone.utc)
                    if st.reactivated_at.tzinfo is None
                    else st.reactivated_at
                )
                if react_utc <= last_digest_created_at:
                    continue
        unarchived_topics.append(t)

    window_cutoff = utcnow() - timedelta(hours=recent_hours)
    fresh_topics = [
        t
        for t in unarchived_topics
        if (
            t.last_seen_at is not None
            and (
                t.last_seen_at.replace(tzinfo=timezone.utc)
                if t.last_seen_at.tzinfo is None
                else t.last_seen_at
            )
            >= window_cutoff
        )
        or bool(state_map.get(t.id) and state_map[t.id].has_updates)
    ]
    candidate_topics = fresh_topics if len(fresh_topics) >= 3 else unarchived_topics

    scored_by_cat: dict[str, list[tuple[float, Topic]]] = defaultdict(list)
    for t in candidate_topics:
        vote = feedback_map.get(t.id)
        rel = compute_topic_relevance(t, profile, vote)
        if rel < 0:
            continue
        scored_by_cat[t.category_slug].append((rel, t))

    for cat_list in scored_by_cat.values():
        cat_list.sort(key=lambda pair: (pair[0], pair[1].last_seen_at), reverse=True)

    # Garantit une sélection concentrée et équilibrée (2 à 4 meilleurs sujets par catégorie active)
    selected: list[tuple[float, Topic]] = []
    seen_ids: set[UUID] = set()
    quota_per_cat = max(2, min(4, max_topics // max(1, len(scored_by_cat))))
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

    from morning_review.pipeline.synthesizer import (
        build_topic_structured_snapshot,
        ensure_topic_full_content_and_llm_summary,
        get_deduplicated_topic_articles,
    )

    is_pytest = bool(os.environ.get("PYTEST_CURRENT_TEST"))

    for rel_score, t in selected:
        if not is_pytest:
            try:
                ensure_topic_full_content_and_llm_summary(session, t, user_id)
            except Exception:
                pass
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
        t_state = state_map.get(t.id)
        has_upd = bool(t_state and t_state.has_updates)
        upd_sum = (t_state.updates_summary or "") if t_state else ""
        # Enregistre le snapshot du sujet présenté dans ce digest afin que toute nouvelle source
        # ultérieure soit évaluée par rapport à ce qui a déjà été présenté à l'utilisateur
        if t_state is None:
            t_state = UserTopicState(
                user_id=user_id,
                topic_id=t.id,
                is_archived=False,
                archived_summary_snapshot=build_topic_structured_snapshot(session, t),
                has_updates=False,
            )
            session.add(t_state)
            state_map[t.id] = t_state
        else:
            t_state.archived_summary_snapshot = build_topic_structured_snapshot(session, t)
            t_state.has_updates = False

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
            "summary": (t.summary or "")[:1200],
            "has_updates": has_upd,
            "updates_summary": upd_sum[:500],
            "subtopics": (t.subtopics_json or [])[:4] if isinstance(t.subtopics_json, list) else [],
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

    from morning_review.core.config import settings
    from morning_review.db.models import NewsletterConfirmation

    recent_cutoff = utcnow() - timedelta(hours=48)
    conf_rows = session.scalars(
        select(NewsletterConfirmation)
        .where(
            NewsletterConfirmation.user_id == user_id,
            NewsletterConfirmation.status != "dismissed",
            or_(
                NewsletterConfirmation.status == "manual_required",
                NewsletterConfirmation.processed_at >= recent_cutoff,
            ),
        )
        .order_by(NewsletterConfirmation.processed_at.desc())
        .limit(15)
    ).all()
    conf_payloads = [
        {
            "sender_name": cr.sender_name,
            "sender_email": cr.sender_email,
            "subject": cr.subject,
            "confirmation_url": cr.confirmation_url,
            "status": cr.status,
            "status_detail": cr.status_detail,
        }
        for cr in conf_rows
    ]
    if conf_rows:
        md_lines.append("## ✉️ Inscriptions Newsletters (Double Opt-In)")
        md_lines.append("")
        for cr in conf_rows:
            if cr.status == "confirmed":
                md_lines.append(
                    f"- ✅ **Auto-validée** : **{cr.sender_name}** (`{cr.sender_email}`) — *{cr.subject}* ({cr.status_detail})"
                )
            else:
                md_lines.append(
                    f"- ⚠️ **Validation 1-clic requise** : **{cr.sender_name}** (`{cr.sender_email}`) — [Cliquer pour confirmer l'inscription]({cr.confirmation_url}) ({cr.status_detail})"
                )
        md_lines.append("")

    for cat_slug, items in sorted(by_category.items()):
        md_lines.append(f"## Catégorie : `{cat_slug}` ({len(items)} sujets)")
        md_lines.append("")
        for item in items:
            upd_prefix = "🔄 **[MISE À JOUR]** " if item.get("has_updates") else ""
            md_lines.append(
                f"### {upd_prefix}[{item['title']}]({item['primary_url']}) "
                f"*(score: {item['relevance_score']} | {item['source_count']} source(s))*"
            )
            md_lines.append(
                f"- **`topic_id`** : `{item['topic_id']}` | "
                f"**1re source** : `{item['first_seen_at'] or 'n/a'}` | "
                f"**Dernière source** : `{item['last_seen_at'] or 'n/a'}`"
            )
            if item.get("has_updates") and item.get("updates_summary"):
                md_lines.append(f"- **Nouveauté détectée** : {item['updates_summary']}")
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

    email_status = "not_requested"
    if send_email:
        if digest_row.emailed_at is not None and not force_send:
            email_status = "already_sent"
        else:
            from morning_review.connectors.gmail_smtp import (
                build_digest_html,
                build_digest_plain_text,
                send_digest_email,
            )

            updates_cnt = sum(1 for it in digest_topics_payload if it.get("has_updates"))
            subj = f"Morning Review — {target_date.isoformat()} ({len(digest_topics_payload)} sujets"
            if updates_cnt:
                subj += f" | {updates_cnt} màj"
            subj += ")"
            plain_body = build_digest_plain_text(
                target_date=target_date,
                by_category=by_category,
                confirmations=conf_payloads,
                ui_url=settings.public_ui_url,
            )
            html_body = build_digest_html(
                target_date=target_date,
                by_category=by_category,
                confirmations=conf_payloads,
                ui_url=settings.public_ui_url,
            )
            sent_info = send_digest_email(
                subject=subj,
                plain_markdown=plain_body,
                html_body=html_body,
                recipient_email=recipient_email,
                smtp_factory=smtp_factory,
            )
            digest_row.emailed_at = utcnow()
            digest_row.emailed_to = sent_info["to"]
            email_status = "sent"

    session.commit()
    return {
        "digest_id": str(digest_row.id),
        "user_id": str(user_id),
        "date": target_date.isoformat(),
        "topics_count": len(digest_topics_payload),
        "s3_key": s3_key,
        "local_report_path": str(local_report_path),
        "categories": {k: len(v) for k, v in by_category.items()},
        "email_status": email_status,
        "emailed_at": digest_row.emailed_at.isoformat() if digest_row.emailed_at else None,
        "emailed_to": digest_row.emailed_to,
    }
