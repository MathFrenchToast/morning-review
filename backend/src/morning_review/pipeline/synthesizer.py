from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import parse_qs, quote_plus, urlparse
from uuid import UUID

import feedparser
import httpx
import numpy as np
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from morning_review.core.config import settings
from morning_review.core.storage import storage
from morning_review.db.models import (
    CanonicalItem,
    Source,
    SourceMention,
    Topic,
    TopicItem,
    UserTopicState,
    utcnow,
)
from morning_review.pipeline.embeddings import embedding_service

logger = logging.getLogger(__name__)


def _word_set(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9àâçéèêëîïôûùüÿñæœ]{3,}", (text or "").lower()))


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / float(len(a | b))


def _short_source_label(source_title: str) -> str:
    label = (source_title or "Source").split(" - ")[0].split(" (")[0].strip()
    return label[:32]


def _is_tldr_source(source_title: str, sender_email: str | None, source_url: str | None) -> bool:
    if sender_email and "tldrnewsletter.com" in sender_email.lower():
        return True
    if source_url and "tldrnewsletter.com" in source_url.lower():
        return True
    return (source_title or "").upper().startswith("TLDR")


def _get_publisher_family(source_title: str, sender_email: str | None, source_url: str | None) -> str:
    u = (source_url or "").lower()
    t = (source_title or "").lower()
    e = (sender_email or "").lower()
    combined = f"{u} {t} {e}"
    if _is_tldr_source(source_title, sender_email, source_url):
        return "tldr"
    if "radiofrance.fr" in u or "radio france" in t or "france inter" in t or "france culture" in t:
        return "radiofrance"
    if "lemonde.fr" in u or "le monde" in t:
        return "lemonde"
    if "nytimes.com" in u or "new york times" in t:
        return "nytimes"
    if "francetvinfo.fr" in u or "franceinfo" in t:
        return "franceinfo"
    for family_slug, marker in (
        ("bytebytego", "bytebytego"),
        ("pragmaticengineer", "pragmatic"),
        ("latentspace", "latent.space"),
        ("importai", "importai"),
        ("constructionphysics", "construction-physics"),
        ("benevans", "ben-evans"),
    ):
        if marker in combined:
            return family_slug
    if sender_email:
        return f"newsletter:{sender_email.lower()}"
    return _short_source_label(source_title).lower()


def _normalize_article_title_key(title: str) -> str:
    s = re.sub(r"\(\d+\s*minute\s*read\)", "", title or "", flags=re.I)
    s = re.sub(r"\(GitHub\s*Repo\)", "", s, flags=re.I)
    words = re.findall(r"[a-z0-9àâçéèêëîïôûùüÿñæœ]+", s.lower())
    return " ".join(words)


def get_deduplicated_topic_articles(
    session: Session,
    topic_id: UUID,
) -> list[dict[str, Any]]:
    """Retourne la liste dédoublonnée des articles rattachés à un `topic_id`.
    - Les différentes déclinaisons de TLDR (TLDR, TLDR Dev, TLDR InfoSec, TLDR AI, TLDR DevOps)
      reprenant un même article sont fusionnées en 1 seule entrée (et comptent pour 1 seule source).
    - Une même URL canonique reprise dans deux sous-flux d'un même média (ex: Radio France)
      n'est comptée qu'une seule fois.
    """
    rows = session.execute(
        select(
            CanonicalItem.title,
            CanonicalItem.canonical_url,
            CanonicalItem.excerpt,
            CanonicalItem.published_at,
            Source.title,
            Source.kind,
            Source.sender_email,
            Source.url,
            Source.metadata_json,
        )
        .join(SourceMention, SourceMention.item_id == CanonicalItem.id)
        .join(Source, Source.id == SourceMention.source_id)
        .where(CanonicalItem.topic_id == topic_id)
        .order_by(CanonicalItem.published_at.desc())
    ).all()

    dedup_map: dict[str, dict[str, Any]] = {}
    for item_title, item_url, item_excerpt, item_pub_at, src_title, src_kind, sender_email, src_url, src_meta in rows:
        is_tldr = _is_tldr_source(src_title or "", sender_email, src_url)
        pub_family = _get_publisher_family(src_title or "", sender_email, src_url)
        norm_title_key = _normalize_article_title_key(item_title or "")
        meta_dict = src_meta or {}
        src_labels = list(meta_dict.get("labels") or [])
        src_creator = str(meta_dict.get("creator") or "").strip()
        if is_tldr:
            # Toutes les déclinaisons TLDR pour un même article partagent la même clé
            dedup_key = f"tldr::{norm_title_key[:80]}"
            edition_name = (src_title or "TLDR").split("(")[0].strip()
        else:
            # Dédoublonne une même URL au sein d'une même famille d'éditeur (ex: sous-flux Radio France)
            dedup_key = f"{pub_family}::{(item_url or norm_title_key).lower()}"
            edition_name = (src_title or "Source").strip()

        if dedup_key not in dedup_map:
            dedup_map[dedup_key] = {
                "title": re.sub(r"\s*\(\d+\s*minute\s*read\)", "", item_title or "", flags=re.I).strip() or item_title,
                "url": item_url,
                "excerpt": item_excerpt or "",
                "published_at": item_pub_at,
                "source_title": edition_name if not is_tldr else "TLDR",
                "editions": [edition_name],
                "kind": src_kind,
                "is_tldr": is_tldr,
                "labels": src_labels,
                "creator": src_creator,
            }
        else:
            entry = dedup_map[dedup_key]
            if edition_name not in entry["editions"]:
                entry["editions"].append(edition_name)
            for lb in src_labels:
                if lb not in entry["labels"]:
                    entry["labels"].append(lb)
            if not entry.get("creator") and src_creator:
                entry["creator"] = src_creator
            if item_excerpt and len(item_excerpt) > len(entry["excerpt"]):
                entry["excerpt"] = item_excerpt
            if item_pub_at and (entry["published_at"] is None or item_pub_at > entry["published_at"]):
                entry["published_at"] = item_pub_at
            # Préfère une URL directe plutôt qu'un lien court links.tldrnewsletter.com
            if item_url and "links.tldrnewsletter.com" not in item_url and "links.tldrnewsletter.com" in (entry["url"] or ""):
                entry["url"] = item_url

    result: list[dict[str, Any]] = []
    for entry in dedup_map.values():
        if entry["is_tldr"]:
            editions_str = ", ".join(entry["editions"])
            display_src = f"TLDR ({editions_str})" if len(entry["editions"]) > 1 else entry["editions"][0]
        else:
            display_src = " / ".join(entry["editions"][:2])
        pub_dt = entry.get("published_at")
        result.append(
            {
                "title": entry["title"],
                "url": entry["url"],
                "excerpt": entry["excerpt"],
                "source_title": display_src,
                "kind": entry["kind"],
                "labels": entry.get("labels") or [],
                "creator": entry.get("creator") or "",
                "published_at": pub_dt.isoformat() if pub_dt else None,
            }
        )
    return result


def sync_topic_source_dates(session: Session, topic: Topic) -> None:
    """Synchronise `topic.first_seen_at` (date de la première source) et `topic.last_seen_at`
    (date de la source la plus récente) à partir de `MIN/MAX(CanonicalItem.published_at)`.
    """
    bounds = session.execute(
        select(
            func.min(CanonicalItem.published_at),
            func.max(CanonicalItem.published_at),
        ).where(CanonicalItem.topic_id == topic.id)
    ).one_or_none()
    if bounds is not None:
        min_pub, max_pub = bounds
        if min_pub is not None:
            topic.first_seen_at = min_pub
        if max_pub is not None:
            topic.last_seen_at = max_pub


def _call_openrouter_synthesis(title: str, source_snippets: list[str]) -> str | None:
    """Appelle OpenRouter (:free) si OPENROUTER_API_KEY est renseignée, sinon retourne None."""
    api_key = (settings.openrouter_api_key or "").strip()
    if not api_key or api_key.startswith("sk-or-v1-replace"):
        return None

    joined = "\n".join(f"- {s}" for s in source_snippets[:8])
    prompt = (
        f"Sujet principal : {title}\n\n"
        f"Extraits des différentes sources :\n{joined}\n\n"
        "Rédige en français une synthèse factuelle, dense et directe (3 à 4 phrases maximum, sans intro "
        "superflue) qui croise les faits et les angles complémentaires de ces sources en citant "
        "les sources entre crochets (ex: [Le Monde], [Franceinfo])."
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://localhost:8000",
        "X-Title": "Morning Review",
    }

    with httpx.Client(timeout=18.0) as client:
        for model_slug in settings.openrouter_free_models:
            try:
                resp = client.post(
                    f"{settings.openrouter_base_url.rstrip('/')}/chat/completions",
                    headers=headers,
                    json={
                        "model": model_slug,
                        "messages": [
                            {
                                "role": "system",
                                "content": "Tu es un analyste de veille factuel et concis.",
                            },
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.2,
                        "max_tokens": 280,
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    content = (
                        data.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")
                        .strip()
                    )
                    if len(content) >= 40:
                        return content
            except Exception as exc:
                logger.debug("OpenRouter synthesis fallback (%s): %s", model_slug, exc)
    return None


def _build_deterministic_multisource_summary(
    topic_title: str,
    rows: list[tuple[str, str, str]],
) -> str:
    """Synthèse extractive déterministe croisant les angles complémentaires de plusieurs sources.
    Priorise un point par média distinct avant d'ajouter un second angle d'un même média.
    """
    all_distinct_sources = list(dict.fromkeys(_short_source_label(r[2]) for r in rows))

    # Réordonne les lignes pour alterner les sources distinctes en priorité (round-robin par source)
    by_src: dict[str, list[tuple[str, str, str]]] = {}
    for r in rows:
        lbl = _short_source_label(r[2])
        by_src.setdefault(lbl, []).append(r)

    interleaved_rows: list[tuple[str, str, str]] = []
    max_len = max((len(v) for v in by_src.values()), default=0)
    for idx in range(max_len):
        for lbl in all_distinct_sources:
            if idx < len(by_src[lbl]):
                interleaved_rows.append(by_src[lbl][idx])

    selected_points: list[str] = []
    seen_word_sets: list[set[str]] = []

    for item_title, item_excerpt, src_title in interleaved_rows:
        src_label = _short_source_label(src_title)
        candidate_text = (item_excerpt or "").strip()

        # Nettoie d'éventuels liens Markdown dans le premier extrait pour garder un résumé lisible
        candidate_text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", candidate_text)
        candidate_text = re.sub(r"^\[+", "", candidate_text).strip()

        if not candidate_text or len(candidate_text) < 25:
            candidate_text = item_title.strip()
        else:
            first_sent = re.split(r"(?<=[.!?])\s+", candidate_text)[0].strip()
            candidate_text = first_sent if len(first_sent) >= 35 else candidate_text[:220]

        candidate_text = re.sub(r"\(\d+\s*minute\s*read\)", "", candidate_text, flags=re.I).strip()
        wset = _word_set(candidate_text)
        if not wset:
            continue

        if any(_jaccard(wset, prev) >= 0.50 for prev in seen_word_sets):
            continue

        seen_word_sets.append(wset)
        selected_points.append(f"[{src_label}] {candidate_text.rstrip('.')}.")
        if len(selected_points) >= 4:
            break

    if not selected_points:
        return topic_title

    return (" • ".join(selected_points))[:900]


def synthesize_topic_summary(session: Session, topic: Topic) -> str:
    """Génère et met à jour `topic.summary` en croisant les sources indépendantes rattachées au `topic_id`."""
    from morning_review.pipeline.ingestion import _extract_url_slug_tokens

    sync_topic_source_dates(session, topic)
    dedup_articles = get_deduplicated_topic_articles(session, topic.id)
    topic.source_count = max(1, len(dedup_articles))

    if topic.summary:
        topic.summary = re.sub(
            r"^Synthèse\s+(?:croisée|IA)\s*\([^)]+\)\s*:\s*",
            "",
            topic.summary,
            flags=re.IGNORECASE,
        ).strip()

    if len(dedup_articles) <= 1:
        if dedup_articles and dedup_articles[0]["excerpt"]:
            # Si le résumé précédent contenait un assemblage multi-sources obsolète, restaure l'extrait propre
            if " • [" in (topic.summary or ""):
                topic.summary = dedup_articles[0]["excerpt"]
    else:
        snippets = [
            f"[{_short_source_label(a['source_title'])}] Titre: {a['title']} | Extrait: {(a['excerpt'] or '')[:260]}"
            for a in dedup_articles
        ]
        llm_summary = _call_openrouter_synthesis(topic.title, snippets)
        if llm_summary:
            topic.summary = llm_summary[:900]
        else:
            clean_rows = [(a["title"], a["excerpt"], a["source_title"]) for a in dedup_articles]
            topic.summary = _build_deterministic_multisource_summary(topic.title, clean_rows)

    extra_fts_parts = [_extract_url_slug_tokens(topic.primary_url)]
    for a in dedup_articles[:10]:
        extra_fts_parts.append(a.get("title") or "")
        extra_fts_parts.append(_extract_url_slug_tokens(a.get("url")))
    fts_blob = " ".join([topic.title or "", topic.summary or "", *extra_fts_parts]).strip()
    topic.fts = func.to_tsvector("simple", fts_blob[:4000])
    return topic.summary or topic.title


_NOVELTY_STOPWORDS: set[str] = {
    "avec", "dans", "pour", "plus", "sont", "cette", "leur", "leurs", "tout", "tous",
    "comme", "mais", "elle", "elles", "nous", "vous", "être", "avoir", "fait", "faire",
    "après", "avant", "entre", "aussi", "ainsi", "selon", "contre", "vers", "chez",
    "depuis", "alors", "très", "bien", "sans", "sous", "dont", "quand", "comment",
    "that", "this", "with", "from", "have", "been", "were", "they", "their", "what",
    "when", "where", "which", "while", "about", "into", "over", "after", "before",
    "more", "most", "some", "such", "than", "then", "them", "these", "those", "will",
    "would", "could", "should", "also", "just", "only", "other", "first", "new",
    "article", "source", "minute", "read", "https", "http",
}


def _clean_text_for_novelty(raw: str) -> str:
    txt = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", raw or "")
    txt = re.sub(r"https?://\S+", "", txt)
    txt = re.sub(r"^#+\s+.*$", "", txt, flags=re.MULTILINE)
    txt = re.sub(r"^-\s+\*\*(?:Source|URL|Publié)[^*]*\*\*.*$", "", txt, flags=re.MULTILINE)
    txt = re.sub(r"\(\d+\s*minute\s*read\)", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def evaluate_topic_novelty(
    archived_summary_snapshot: str | None,
    *,
    new_title: str,
    new_text: str,
    source_title: str = "",
) -> tuple[bool, list[str], str]:
    """Vérifie si un nouvel article (`new_title` + `new_text`) apporte des éléments clés inédits
    qui ne figurent pas déjà dans `archived_summary_snapshot`.
    Retourne `(is_novel, novel_points, formatted_update_text)`.
    """
    base_snapshot = _clean_text_for_novelty(archived_summary_snapshot or "")
    src_label = _short_source_label(source_title or "Nouvelle source")

    old_segments = [
        seg.strip()
        for seg in re.split(r"(?:•|\n|(?<=[.!?])\s+)", base_snapshot)
        if len(seg.strip()) >= 15
    ]
    old_word_sets = [_word_set(seg) for seg in old_segments if _word_set(seg)]
    all_old_words = _word_set(base_snapshot)
    old_numbers = set(re.findall(r"\b\d+(?:[.,]\d+)?%?\b", base_snapshot))

    clean_new_body = _clean_text_for_novelty(new_text or "")
    clean_new_title = _clean_text_for_novelty(new_title or "")

    candidate_sentences: list[str] = []
    for seg in re.split(r"(?:•|\n|(?<=[.!?])\s+)", clean_new_body):
        s_clean = seg.strip(" -•*[]")
        if len(s_clean) >= 28:
            candidate_sentences.append(s_clean)
    if clean_new_title and len(clean_new_title) >= 20:
        candidate_sentences.append(clean_new_title)

    novel_points: list[str] = []
    seen_new_sets: list[set[str]] = []

    for cand in candidate_sentences:
        wset = _word_set(cand)
        if not wset:
            continue
        max_old_jaccard = max((_jaccard(wset, ow) for ow in old_word_sets), default=0.0)
        if max_old_jaccard >= 0.42:
            # Simple reformulation d'un élément déjà présent dans le résumé archivé
            continue
        if any(_jaccard(wset, prev) >= 0.48 for prev in seen_new_sets):
            continue

        novel_words = {
            w for w in wset
            if len(w) >= 4 and w not in _NOVELTY_STOPWORDS and w not in all_old_words
        }
        cand_numbers = set(re.findall(r"\b\d+(?:[.,]\d+)?%?\b", cand))
        novel_numbers = cand_numbers - old_numbers

        if len(novel_words) >= 3 or (len(novel_numbers) >= 1 and len(novel_words) >= 2):
            seen_new_sets.append(wset)
            snippet = cand[:240].rstrip(".") + "."
            novel_points.append(snippet)
            if len(novel_points) >= 2:
                break

    if not novel_points:
        return False, [], ""

    formatted = " • ".join(f"[{src_label}] {pt}" for pt in novel_points)
    return True, novel_points, formatted[:600]


def check_and_reactivate_topic_if_novel(
    session: Session,
    *,
    topic: Topic,
    new_title: str,
    new_text: str,
    source_title: str = "",
) -> bool:
    """Si un sujet (`topic`) a été marqué comme lu/archivé par un utilisateur, vérifie si le
    nouvel article (`new_title` + `new_text`) apporte des éléments inédits par rapport au
    `archived_summary_snapshot`. Si oui, réactive le sujet (`is_archived = False`, `has_updates = True`)
    et enregistre la mention des nouveautés dans `updates_summary`.
    """
    states = session.scalars(
        select(UserTopicState).where(UserTopicState.topic_id == topic.id)
    ).all()
    if not states:
        return False

    reactivated_any = False
    for state in states:
        if not state.is_archived and not state.has_updates:
            continue
        snapshot_ref = state.archived_summary_snapshot or f"{topic.title}\n{topic.summary or ''}"
        is_novel, _, formatted_update = evaluate_topic_novelty(
            snapshot_ref,
            new_title=new_title,
            new_text=new_text,
            source_title=source_title,
        )
        if not is_novel:
            continue

        state.is_archived = False
        state.has_updates = True
        state.new_articles_count = (state.new_articles_count or 0) + 1
        state.reactivated_at = utcnow()
        if state.updates_summary:
            if formatted_update not in state.updates_summary:
                state.updates_summary = f"{state.updates_summary} • {formatted_update}"[:900]
        else:
            state.updates_summary = formatted_update
        # Enrichit également le snapshot de référence pour éviter de ré-alerter deux fois sur la même nouveauté
        state.archived_summary_snapshot = f"{snapshot_ref}\n{formatted_update}"[:4000]
        reactivated_any = True

    return reactivated_any


def set_topic_archive_state(
    session: Session,
    *,
    user_id: UUID,
    topic_id: UUID,
    archived: bool = True,
) -> dict[str, Any]:
    """Marque un `topic_id` comme lu/archivé (`archived=True`) ou le remet dans le flux actif (`archived=False`).
    Lors de l'archivage, enregistre le snapshot du résumé courant afin de détecter les futures nouveautés.
    """
    topic = session.get(Topic, topic_id)
    if topic is None:
        raise ValueError(f"Topic introuvable : {topic_id}")

    state = session.scalar(
        select(UserTopicState).where(
            UserTopicState.user_id == user_id,
            UserTopicState.topic_id == topic_id,
        )
    )
    dedup_articles = get_deduplicated_topic_articles(session, topic.id)
    current_scount = len(dedup_articles) or topic.source_count
    snapshot = f"{topic.title}\n{topic.summary or ''}".strip()

    if state is None:
        state = UserTopicState(
            user_id=user_id,
            topic_id=topic_id,
            is_archived=archived,
            archived_at=utcnow(),
            archived_source_count=current_scount,
            archived_summary_snapshot=snapshot if archived else None,
            has_updates=False,
            updates_summary=None,
            new_articles_count=0,
        )
        session.add(state)
    else:
        state.is_archived = archived
        if archived:
            state.archived_at = utcnow()
            state.archived_source_count = current_scount
            state.archived_summary_snapshot = snapshot
            state.has_updates = False
            state.updates_summary = None
            state.new_articles_count = 0
        else:
            state.has_updates = False
            state.updates_summary = None
            state.new_articles_count = 0

    session.commit()
    return {
        "topic_id": str(topic.id),
        "is_archived": state.is_archived,
        "archived_at": state.archived_at.isoformat() if state.archived_at else None,
        "has_updates": state.has_updates,
        "updates_summary": state.updates_summary,
        "new_articles_count": state.new_articles_count,
    }


def _build_search_query(title: str) -> tuple[str, str]:
    """Construit une requête de recherche concise et détecte la langue probable (fr ou en)."""
    cleaned = re.sub(r"\(\d+\s*minute\s*read\)", "", title, flags=re.I)
    cleaned = re.sub(r"\[[^\]]+\]", "", cleaned)
    words = re.findall(r"[A-Za-z0-9À-ÿ'-]{3,}", cleaned)
    fr_markers = {"les", "des", "une", "dans", "pour", "avec", "sur", "selon", "après", "panne", "monde"}
    is_fr = any(w.lower() in fr_markers or any(c in w.lower() for c in "éèêàùçôî") for w in words)
    lang = "fr" if is_fr else "en"
    query_terms = [w for w in words if w.lower() not in {"the", "and", "for", "with", "from", "une", "les", "des", "dans", "pour"}][:7]
    return " ".join(query_terms) or cleaned[:60], lang


def _unwrap_bing_link(raw_link: str) -> str:
    if "bing.com/news/apiclick.aspx" in raw_link:
        parsed = urlparse(raw_link)
        qs = parse_qs(parsed.query)
        if "url" in qs and qs["url"]:
            return qs["url"][0]
    return raw_link


def corroborate_topic_via_web(
    session: Session,
    *,
    user_id: UUID,
    topic_id: UUID,
    max_new_sources: int = 3,
) -> dict[str, Any]:
    """Croise les sources d'un `topic_id` (notamment lorsqu'il n'a qu'une seule source) via une
    recherche d'actualités sur le Web (Google News RSS + Bing News RSS), filtre sémantiquement
    les articles corroborants et génère une synthèse multi-sources.
    """
    from morning_review.pipeline.ingestion import (
        canonicalize_url,
        clean_aggregator_noise,
        compute_hybrid_story_match,
        extract_story_profile,
    )

    topic = session.get(Topic, topic_id)
    if topic is None:
        raise ValueError(f"Topic introuvable : {topic_id}")

    query_str, lang = _build_search_query(topic.title)
    encoded_q = quote_plus(query_str)
    if lang == "fr":
        gnews_url = f"https://news.google.com/rss/search?q={encoded_q}&hl=fr&gl=FR&ceid=FR:fr"
    else:
        gnews_url = f"https://news.google.com/rss/search?q={encoded_q}&hl=en-US&gl=US&ceid=US:en"
    bing_url = f"https://www.bing.com/news/search?q={encoded_q}&format=rss"

    existing_urls = set(
        session.scalars(
            select(CanonicalItem.canonical_url).where(CanonicalItem.topic_id == topic.id)
        ).all()
    )
    existing_titles = {
        (t or "").lower().strip()
        for t in session.scalars(
            select(CanonicalItem.title).where(CanonicalItem.topic_id == topic.id)
        ).all()
    }
    existing_publishers = {
        _short_source_label(s or "").lower()
        for s in session.scalars(
            select(Source.title)
            .join(SourceMention, SourceMention.source_id == Source.id)
            .join(CanonicalItem, CanonicalItem.id == SourceMention.item_id)
            .where(CanonicalItem.topic_id == topic.id)
        ).all()
    }

    topic_vec = np.asarray(list(topic.centroid_embedding), dtype=np.float32)
    topic_vec = topic_vec / (float(np.linalg.norm(topic_vec)) or 1.0)
    topic_subj, topic_concepts = extract_story_profile(topic.title, topic.summary or "")

    candidates: list[dict[str, Any]] = []
    with httpx.Client(
        follow_redirects=True,
        timeout=10.0,
        headers={"User-Agent": "MorningReview/1.0 (RSS Reader; Feed Fetcher)"},
    ) as client:
        for feed_url in (gnews_url, bing_url):
            try:
                resp = client.get(feed_url)
                if resp.status_code >= 400:
                    continue
                parsed = feedparser.parse(resp.text)
                for entry in (parsed.entries or [])[:12]:
                    raw_title = getattr(entry, "title", "") or ""
                    # Google News RSS ajoute souvent " - Nom du Média" à la fin du titre
                    publisher = "Web News"
                    src_obj = getattr(entry, "source", None)
                    if isinstance(src_obj, dict) and src_obj.get("title"):
                        publisher = src_obj["title"].strip()
                    if " - " in raw_title:
                        parts = raw_title.rsplit(" - ", 1)
                        if len(parts[1]) < 35:
                            raw_title = parts[0].strip()
                            if publisher == "Web News":
                                publisher = parts[1].strip()

                    link = _unwrap_bing_link(getattr(entry, "link", "") or "")
                    norm_link = canonicalize_url(link)
                    if not norm_link or norm_link in existing_urls:
                        continue
                    if raw_title.lower().strip() in existing_titles:
                        continue

                    raw_desc = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
                    clean_desc = re.sub(r"<[^>]+>", " ", raw_desc)
                    clean_desc = clean_aggregator_noise(" ".join(clean_desc.split()))
                    if clean_desc.lower().startswith(raw_title.lower()[:40]):
                        clean_desc = ""

                    candidates.append(
                        {
                            "title": raw_title,
                            "publisher": publisher,
                            "url": norm_link,
                            "excerpt": clean_desc,
                        }
                    )
            except Exception as exc:
                logger.warning("Web corroboration fetch warning (%s): %s", feed_url, exc)

    added_articles: list[dict[str, Any]] = []
    seen_publishers: set[str] = set(existing_publishers)

    for cand in candidates:
        if len(added_articles) >= max_new_sources:
            break
        pub_key = cand["publisher"].lower()
        if pub_key in seen_publishers:
            continue

        cand_text = f"{cand['title']}\n{cand['excerpt']}".strip()
        cand_vec = np.asarray(embedding_service.embed_one(cand_text), dtype=np.float32)
        cand_vec = cand_vec / (float(np.linalg.norm(cand_vec)) or 1.0)
        cos_sim = float(np.dot(topic_vec, cand_vec))

        cand_subj, cand_concepts = extract_story_profile(cand["title"], cand["excerpt"])
        matched, hyb_score = compute_hybrid_story_match(
            cos_sim,
            topic_subj,
            topic_concepts,
            cand_subj,
            cand_concepts,
        )
        # Autorise aussi une correspondance directe sur requête ciblée si cos >= 0.48 et >= 1 sujet partagé
        if not matched and not (cos_sim >= 0.48 and len(topic_subj & cand_subj) >= 1):
            continue

        seen_publishers.add(pub_key)
        existing_urls.add(cand["url"])
        existing_titles.add(cand["title"].lower().strip())

        src_url = f"web://corroboration/{re.sub(r'[^a-z0-9]+', '-', pub_key).strip('-') or 'news'}"
        web_source = session.scalar(select(Source).where(Source.url == src_url))
        if web_source is None:
            web_source = Source(
                kind="web",
                url=src_url,
                title=f"{cand['publisher']} (Croisement Web)",
                is_public=True,
            )
            session.add(web_source)
            session.flush()

        md_body = (
            f"# {cand['title']}\n\n"
            f"- **Source croisée** : {cand['publisher']}\n"
            f"- **URL** : {cand['url']}\n\n"
            f"{cand['excerpt'] or cand['title']}\n"
        )
        sha256_hex = storage.compute_sha256(md_body)
        s3_key = storage.build_item_key(sha256_hex, visibility="public", user_id=None)
        storage.put_markdown(s3_key, md_body)

        item = session.scalar(
            select(CanonicalItem).where(
                CanonicalItem.canonical_url == cand["url"],
                CanonicalItem.visibility == "public",
            )
        )
        if item is None:
            item = CanonicalItem(
                visibility="public",
                user_id=None,
                topic_id=topic.id,
                canonical_url=cand["url"],
                title=cand["title"][:500],
                excerpt=(cand["excerpt"] or cand["title"])[:500],
                category_slug=topic.category_slug,
                content_sha256=sha256_hex,
                s3_key=s3_key,
                published_at=utcnow(),
                embedding=cand_vec.tolist(),
                embedding_model_version=embedding_service.model_version,
                fts=func.to_tsvector("simple", f"{cand['title']} {cand['excerpt']}"),
            )
            session.add(item)
            session.flush()
        else:
            item.topic_id = topic.id

        if (
            session.scalar(
                select(TopicItem).where(
                    TopicItem.topic_id == topic.id,
                    TopicItem.item_id == item.id,
                )
            )
            is None
        ):
            session.add(
                TopicItem(
                    topic_id=topic.id,
                    item_id=item.id,
                    similarity_score=hyb_score,
                    source_role="web_corroboration",
                )
            )

        ext_id = cand["url"][:500]
        if (
            session.scalar(
                select(SourceMention).where(
                    SourceMention.source_id == web_source.id,
                    SourceMention.external_id == ext_id,
                )
            )
            is None
        ):
            session.add(
                SourceMention(
                    source_id=web_source.id,
                    item_id=item.id,
                    user_id=None,
                    external_id=ext_id,
                    raw_url=cand["url"][:1024],
                )
            )
            web_source.item_count = (web_source.item_count or 0) + 1

        check_and_reactivate_topic_if_novel(
            session,
            topic=topic,
            new_title=cand["title"],
            new_text=cand["excerpt"] or cand["title"],
            source_title=cand["publisher"],
        )

        added_articles.append(
            {
                "title": cand["title"],
                "publisher": cand["publisher"],
                "url": cand["url"],
                "cosine_similarity": round(cos_sim, 4),
            }
        )

    session.flush()
    m_count = session.scalar(
        select(func.count(SourceMention.id))
        .join(CanonicalItem, CanonicalItem.id == SourceMention.item_id)
        .where(CanonicalItem.topic_id == topic.id)
    )
    topic.source_count = max(1, int(m_count or 1))
    new_summary = synthesize_topic_summary(session, topic)
    session.commit()

    return {
        "topic_id": str(topic.id),
        "title": topic.title,
        "search_query": query_str,
        "corroborating_sources_added": len(added_articles),
        "total_source_count": topic.source_count,
        "synthesized_summary": new_summary,
        "added_articles": added_articles,
    }
