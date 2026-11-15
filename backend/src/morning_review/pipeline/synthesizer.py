from __future__ import annotations

import json
import logging
import os
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
    *,
    include_web_corroboration: bool = False,
) -> list[dict[str, Any]]:
    """Retourne la liste dédoublonnée des articles rattachés à un `topic_id`.
    - Par défaut (`include_web_corroboration=False`), exclut les résultats de recherche web
      (`web://corroboration/...` / `(Croisement Web)`), qui constituent des sujets connexes
      et ne doivent pas polluer le résumé principal du sujet surveillé.
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

    if not include_web_corroboration:
        non_web_rows = [
            r
            for r in rows
            if not (
                str(r[7] or "").startswith("web://corroboration/")
                or "(croisement web)" in str(r[4] or "").lower()
            )
        ]
        if non_web_rows:
            rows = non_web_rows

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

        from morning_review.pipeline.ingestion import clean_aggregator_noise

        cleaned_item_excerpt = clean_aggregator_noise(item_excerpt or "")
        if cleaned_item_excerpt.lower() == (item_title or "").strip().lower():
            cleaned_item_excerpt = ""

        if dedup_key not in dedup_map:
            dedup_map[dedup_key] = {
                "title": re.sub(r"\s*\(\d+\s*minute\s*read\)", "", item_title or "", flags=re.I).strip() or item_title,
                "url": item_url,
                "excerpt": cleaned_item_excerpt,
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
            if cleaned_item_excerpt and len(cleaned_item_excerpt) > len(entry["excerpt"]):
                entry["excerpt"] = cleaned_item_excerpt
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
    """Synchronise `topic.first_seen_at` (date de la première source surveillée) et `topic.last_seen_at`
    (date de la source surveillée la plus récente) à partir de `MIN/MAX(CanonicalItem.published_at)`,
    en excluant les résultats de recherche web (`web://corroboration/...`).
    """
    bounds = session.execute(
        select(
            func.min(CanonicalItem.published_at),
            func.max(CanonicalItem.published_at),
        )
        .join(SourceMention, SourceMention.item_id == CanonicalItem.id)
        .join(Source, Source.id == SourceMention.source_id)
        .where(
            CanonicalItem.topic_id == topic.id,
            ~Source.url.like("web://corroboration/%"),
        )
    ).one_or_none()
    if bounds is None or (bounds[0] is None and bounds[1] is None):
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


def _get_openrouter_models() -> list[str]:
    primary = getattr(settings, "openrouter_primary_model", None) or "openrouter/free"
    models = [
        primary,
        "google/gemini-2.0-flash-exp:free",
        "meta-llama/llama-3.3-70b-instruct:free",
    ]
    return list(dict.fromkeys(models))


def _call_llm_json(
    system_prompt: str,
    user_prompt: str,
    *,
    timeout: float = 18.0,
    max_tokens: int = 850,
    allow_local_ollama: bool = True,
) -> dict[str, Any] | None:
    """Appelle un LLM en mode JSON structuré :
    1. OpenRouter si `OPENROUTER_API_KEY` est configurée.
    2. Ollama local (`http://localhost:11434/api/chat` avec `format="json"`) si disponible.
    """
    api_key = (settings.openrouter_api_key or "").strip()
    if api_key and not api_key.startswith("sk-or-v1-replace"):
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://localhost:8000",
            "X-Title": "Morning Review",
        }
        with httpx.Client(timeout=timeout) as client:
            for model_slug in _get_openrouter_models():
                try:
                    resp = client.post(
                        f"{settings.openrouter_base_url.rstrip('/')}/chat/completions",
                        headers=headers,
                        json={
                            "model": model_slug,
                            "messages": [
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": user_prompt},
                            ],
                            "response_format": {"type": "json_object"},
                            "temperature": 0.1,
                            "max_tokens": max_tokens,
                        },
                    )
                    if resp.status_code == 200:
                        content = (
                            resp.json()
                            .get("choices", [{}])[0]
                            .get("message", {})
                            .get("content", "")
                            .strip()
                        )
                        content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.I).strip()
                        parsed = json.loads(content)
                        if isinstance(parsed, dict):
                            return parsed
                except Exception as exc:
                    logger.debug("OpenRouter JSON fallback (%s): %s", model_slug, exc)

    if not allow_local_ollama:
        return None
    # Évite d'appeler le démon Ollama local pendant les tests unitaires sauf si explicitement demandé
    if os.environ.get("PYTEST_CURRENT_TEST") and os.environ.get("MORNING_REVIEW_FORCE_LLM") != "1":
        return None

    ollama_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
    ollama_models = [
        os.environ.get("OLLAMA_MODEL", "qwen2.5:7b"),
        "llama3.2:latest",
        "gemma3:4b",
    ]
    with httpx.Client(timeout=timeout) as client:
        for model_name in dict.fromkeys(ollama_models):
            try:
                resp = client.post(
                    f"{ollama_url}/api/chat",
                    json={
                        "model": model_name,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        "format": "json",
                        "stream": False,
                        "options": {"temperature": 0.1, "num_predict": max_tokens},
                    },
                )
                if resp.status_code == 200:
                    raw_msg = resp.json().get("message", {}).get("content", "").strip()
                    raw_msg = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw_msg, flags=re.I).strip()
                    parsed = json.loads(raw_msg)
                    if isinstance(parsed, dict):
                        return parsed
            except Exception as exc:
                logger.debug("Ollama JSON fallback (%s): %s", model_name, exc)
    return None


def _call_llm_topic_synthesis_and_subtopics(
    title: str,
    dedup_articles: list[dict[str, Any]],
    *,
    allow_local_ollama: bool = True,
) -> dict[str, Any] | None:
    """Analyse et synthétise par LLM (OpenRouter ou Ollama local `qwen2.5:7b`) l'ensemble des sources
    d'un sujet pour produire :
    1. `summary` : une synthèse analytique complète en français (3 à 5 phrases denses) couvrant
       le cœur du sujet, sa chronologie/durée, son état de résolution éventuel et ses implications,
       en citant les sources entre crochets (ex: `[Radio France]`, `[Microsoft 365 Status]`, `[TechRadar]`).
    2. `subtopics` : 2 à 5 sous-sujets thématiques distincts (`title`, `summary`, `sources`) découpant
       analytiquement le dossier (et non une simple recopie des titres d'articles).
    """
    from morning_review.pipeline.ingestion import clean_aggregator_noise

    if not dedup_articles:
        return None

    source_blocks: list[str] = []
    valid_source_labels: list[str] = []
    for idx, art in enumerate(dedup_articles[:10], start=1):
        lbl = _short_source_label(art.get("source_title") or "Source")
        if lbl not in valid_source_labels:
            valid_source_labels.append(lbl)
        pub = art.get("published_at")
        pub_str = (
            pub.strftime("%Y-%m-%d %H:%M UTC")
            if hasattr(pub, "strftime")
            else (str(pub) if pub else "date inconnue")
        )
        art_title = clean_aggregator_noise(art.get("title") or "").strip()
        art_excerpt = clean_aggregator_noise(art.get("excerpt") or "").strip()
        max_len = 2400 if len(dedup_articles) == 1 else 800
        source_blocks.append(
            f"Source #{idx} [{lbl}] (publié: {pub_str})\n"
            f"  Titre: {art_title}\n"
            f"  Contenu: {art_excerpt[:max_len]}"
        )

    system_prompt = (
        "Tu es un rédacteur en chef et analyste de veille stratégique francophone.\n"
        "Ta mission est d'analyser l'ensemble des sources d'un même dossier d'actualité et de produire "
        "une synthèse de fond directement exploitable sans avoir besoin d'ouvrir les articles sources.\n"
        "RÈGLES STRICTES :\n"
        "1. Rédige TOUT en français (traduis et intègre les faits des sources anglophones).\n"
        "2. `summary` (3 à 5 phrases denses et fluides, 350 à 850 caractères) :\n"
        "   - Expose d'abord le cœur factuel de l'événement (nature, ampleur, dates/durée précise, acteurs).\n"
        "   - Indique clairement l'évolution chronologique et l'état actuel (ex: incident résolu / service rétabli avec date et heure de fin si disponible).\n"
        "   - Mentionne les conséquences concrètes et les enjeux/réactions soulevés par les différentes sources (ex: impact sur les utilisateurs, réactions politiques ou souveraineté européenne).\n"
        "   - Cite les sources pertinentes entre crochets dans le texte (ex: [Radio France], [Microsoft 365 Status], [TechRadar]).\n"
        "3. `subtopics` (liste de 2 à 5 objets JSON) :\n"
        "   - Chaque sous-sujet DOIT représenter un AXE THÉMATIQUE ou CHRONOLOGIQUE distinct du dossier "
        "(ex: 'Annonce, durée et ampleur de la panne (12-13 nov.)', 'Rétablissement officiel du service (14 nov.)', 'Enjeux de souveraineté européenne face aux géants cloud'), "
        "et NON une simple copie du titre d'un article.\n"
        "   - Chaque élément de `subtopics` doit avoir les clés : `title` (intitulé thématique clair, 4 à 12 mots), "
        "`summary` (1 à 2 phrases factuelles résumant cet axe avec dates/chiffres clés), et `sources` (liste des noms courts des sources couvrant cet axe).\n"
        "Réponds UNIQUEMENT en JSON valide respectant exactement le schéma :\n"
        "{\"summary\": \"...\", \"subtopics\": [{\"title\": \"...\", \"summary\": \"...\", \"sources\": [\"...\"]}]}"
    )

    user_prompt = (
        f"Dossier principal : {title}\n\n"
        f"Sources disponibles ({len(source_blocks)}) :\n"
        + "\n\n".join(source_blocks)
        + "\n\nProduis l'analyse JSON complète (`summary` + `subtopics`) en français."
    )

    res = _call_llm_json(
        system_prompt,
        user_prompt,
        timeout=28.0,
        max_tokens=900,
        allow_local_ollama=allow_local_ollama,
    )
    if not isinstance(res, dict):
        return None

    raw_summary = clean_aggregator_noise(str(res.get("summary") or "")).strip()
    if len(raw_summary) < 60:
        return None

    raw_subtopics = res.get("subtopics")
    cleaned_subtopics: list[dict[str, Any]] = []
    if isinstance(raw_subtopics, list):
        for item in raw_subtopics[:5]:
            if not isinstance(item, dict):
                continue
            st_title = clean_aggregator_noise(str(item.get("title") or "")).strip()
            st_sum = clean_aggregator_noise(str(item.get("summary") or "")).strip()
            st_srcs = item.get("sources")
            if not st_title or not st_sum:
                continue
            norm_srcs: list[str] = []
            if isinstance(st_srcs, list):
                for s in st_srcs:
                    s_clean = str(s).strip(" []")
                    if s_clean:
                        norm_srcs.append(_short_source_label(s_clean))
            if not norm_srcs and valid_source_labels:
                norm_srcs = [valid_source_labels[0]]
            cleaned_subtopics.append(
                {
                    "title": st_title[:120],
                    "summary": st_sum[:320],
                    "sources": list(dict.fromkeys(norm_srcs))[:5],
                }
            )

    return {
        "summary": raw_summary[:1000],
        "subtopics": cleaned_subtopics,
    }


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
        for model_slug in _get_openrouter_models():
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


def extract_topic_subtopics(
    session: Session,
    topic: Topic,
    dedup_articles: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Extrait la liste structurée des sous-sujets (`subtopics`) couverts par un sujet (`topic_id`).
    Retourne en priorité les sous-sujets thématiques générés par l'analyse LLM (`topic.subtopics_json`),
    ou à défaut effectue un découpage déterministe par facettes.
    Chaque sous-sujet regroupe un angle distinct ou une facette factuelle avec :
    - `title` : intitulé concis du sous-sujet
    - `summary` : synthèse factuelle de cet angle
    - `sources` : liste des médias couvrant ce sous-sujet
    """
    from morning_review.pipeline.ingestion import clean_aggregator_noise

    cached_llm_subtopics = getattr(topic, "subtopics_json", None)
    if isinstance(cached_llm_subtopics, list) and cached_llm_subtopics:
        valid_cached = [
            st
            for st in cached_llm_subtopics
            if isinstance(st, dict) and st.get("title") and st.get("summary")
        ]
        if valid_cached:
            return valid_cached[:5]

    articles = dedup_articles if dedup_articles is not None else get_deduplicated_topic_articles(session, topic.id)
    if not articles:
        return []

    subtopics: list[dict[str, Any]] = []
    cluster_wsets: list[set[str]] = []

    if len(articles) >= 2:
        for art in articles:
            src_lbl = _short_source_label(art.get("source_title") or "Source")
            raw_title = clean_aggregator_noise(art.get("title") or "").strip()
            raw_excerpt = clean_aggregator_noise(art.get("excerpt") or "").strip()
            raw_excerpt = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", raw_excerpt)
            if raw_excerpt.lower() == raw_title.lower():
                raw_excerpt = ""

            facet_text = f"{raw_title} {raw_excerpt}".strip()
            wset = _word_set(facet_text)
            if not wset:
                continue

            # Cherche si cet article couvre un sous-sujet déjà identifié ou un nouvel angle
            matched_idx = -1
            best_jac = 0.0
            for idx, cw in enumerate(cluster_wsets):
                jac = _jaccard(wset, cw)
                if jac >= 0.34 and jac > best_jac:
                    best_jac = jac
                    matched_idx = idx

            first_sent = ""
            if raw_excerpt:
                sents = [s.strip() for s in re.split(r"(?<=[.!?])\s+", raw_excerpt) if len(s.strip()) >= 25]
                first_sent = sents[0] if sents else raw_excerpt[:220]

            if matched_idx >= 0:
                st = subtopics[matched_idx]
                if src_lbl not in st["sources"]:
                    st["sources"].append(src_lbl)
                if first_sent and len(first_sent) > len(st["summary"]):
                    st["summary"] = first_sent[:240]
                cluster_wsets[matched_idx] = cluster_wsets[matched_idx] | wset
            else:
                st_title = raw_title[:110] if raw_title else (first_sent[:90] or topic.title[:90])
                st_summary = first_sent[:240] if first_sent else st_title
                subtopics.append(
                    {
                        "title": st_title,
                        "summary": st_summary,
                        "sources": [src_lbl],
                    }
                )
                cluster_wsets.append(wset)
            if len(subtopics) >= 5:
                break
    else:
        # Sujet mono-source : découpe les facettes / paragraphes distincts si le résumé est riche
        art = articles[0]
        src_lbl = _short_source_label(art.get("source_title") or "Source")
        clean_sum = clean_aggregator_noise(topic.summary or art.get("excerpt") or "")
        sents = [s.strip() for s in re.split(r"(?:•|\n|(?<=[.!?])\s+)", clean_sum) if len(s.strip()) >= 35]
        for s in sents[:3]:
            sw = _word_set(s)
            if any(_jaccard(sw, prev) >= 0.45 for prev in cluster_wsets):
                continue
            cluster_wsets.append(sw)
            short_head = s.split(",")[0].split(":")[0].strip()[:85]
            subtopics.append(
                {
                    "title": short_head if len(short_head) >= 18 else s[:75],
                    "summary": s[:240],
                    "sources": [src_lbl],
                }
            )

    return subtopics[:5]


def build_topic_structured_snapshot(
    session: Session,
    topic: Topic,
    dedup_articles: list[dict[str, Any]] | None = None,
) -> str:
    """Construit le snapshot structuré complet (Titre + Résumé + Sous-sujets couverts)
    enregistré lors du 'Mark as Read' pour servir de référence à l'analyse LLM de nouveauté.
    """
    from morning_review.pipeline.ingestion import clean_aggregator_noise

    articles = dedup_articles if dedup_articles is not None else get_deduplicated_topic_articles(session, topic.id)
    subtopics = extract_topic_subtopics(session, topic, articles)
    clean_sum = clean_aggregator_noise(topic.summary or "")
    lines = [
        f"TITRE: {topic.title}",
        f"RÉSUMÉ: {clean_sum}",
    ]
    if subtopics:
        lines.append("SOUS-SUJETS DÉJÀ COUVERTS:")
        for st in subtopics:
            srcs = ", ".join(st.get("sources") or [])
            lines.append(f"- [{srcs}] {st['title']} : {st['summary']}")
    return "\n".join(lines)[:4000]


def _build_deterministic_multisource_summary(
    topic_title: str,
    rows: list[tuple[str, str, str]],
) -> str:
    """Synthèse extractive déterministe croisant les angles complémentaires de plusieurs sources.
    Priorise un point par média distinct avant d'ajouter un second angle d'un même média,
    en garantissant un texte propre et lisible (sans balises Markdown brutes ni répétition creuse du titre).
    """
    from morning_review.pipeline.ingestion import clean_aggregator_noise

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

    # Vérifie si au moins une source possède un véritable extrait descriptif (> 35 car. différent du titre)
    has_rich_excerpts = any(
        len(clean_aggregator_noise(ex or "")) >= 35
        and clean_aggregator_noise(ex or "").lower() != clean_aggregator_noise(ti or "").lower()
        for ti, ex, _ in interleaved_rows
    )

    selected_points: list[str] = []
    seen_word_sets: list[set[str]] = []

    for item_title, item_excerpt, src_title in interleaved_rows:
        src_label = _short_source_label(src_title)
        clean_title_str = clean_aggregator_noise(item_title or "").strip()
        candidate_text = clean_aggregator_noise(item_excerpt or "").strip()

        # Nettoie d'éventuels liens Markdown dans le premier extrait pour garder un résumé lisible
        candidate_text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", candidate_text)
        candidate_text = re.sub(r"^\[+", "", candidate_text).strip()
        candidate_text = re.sub(r"^#+\s*", "", candidate_text).strip()

        if not candidate_text or len(candidate_text) < 25 or candidate_text.lower() == clean_title_str.lower():
            if has_rich_excerpts and _jaccard(_word_set(clean_title_str), _word_set(topic_title)) >= 0.55:
                # Évite d'polluer un résumé riche avec une source qui n'a qu'un titre quasi-identique sans extrait
                continue
            candidate_text = clean_title_str
        else:
            sents = [
                s.strip()
                for s in re.split(r"(?<=[.!?])\s+", candidate_text)
                if len(s.strip()) >= 20
            ]
            if sents:
                acc: list[str] = []
                total_c = 0
                for s in sents[:2]:
                    if total_c + len(s) > 320 and acc:
                        break
                    acc.append(s)
                    total_c += len(s) + 1
                candidate_text = " ".join(acc)
            else:
                candidate_text = candidate_text[:300]

        candidate_text = clean_aggregator_noise(candidate_text).strip()
        wset = _word_set(candidate_text)
        if not wset:
            continue

        if any(_jaccard(wset, prev) >= 0.46 for prev in seen_word_sets):
            continue

        seen_word_sets.append(wset)
        selected_points.append(f"[{src_label}] {candidate_text.rstrip('.')}.")
        if len(selected_points) >= 4:
            break

    if not selected_points:
        return clean_aggregator_noise(topic_title)

    return (" • ".join(selected_points))[:1000]


def synthesize_topic_summary(session: Session, topic: Topic) -> str:
    """Génère et met à jour `topic.summary` en croisant les sources indépendantes rattachées au `topic_id`."""
    from morning_review.pipeline.ingestion import _extract_url_slug_tokens, clean_aggregator_noise

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
        topic.summary = clean_aggregator_noise(topic.summary)

    # Enrichit les extraits des articles avec les paragraphes propres extraits de S3 si disponibles
    url_to_s3: dict[str, str] = {}
    for ci_url, ci_s3 in session.execute(
        select(CanonicalItem.canonical_url, CanonicalItem.s3_key).where(CanonicalItem.topic_id == topic.id)
    ).all():
        if ci_url and ci_s3:
            url_to_s3[ci_url] = ci_s3

    for art in dedup_articles:
        art_url = art.get("url") or ""
        s3_key = url_to_s3.get(art_url)
        raw_ex = art.get("excerpt") or ""
        has_boilerplate = any(
            m in raw_ex.lower()
            for m in ("member-only story", "hi everyone, welcome back", "[entretien", "](http")
        )
        if s3_key and (len(raw_ex) < 350 or has_boilerplate or len(dedup_articles) == 1):
            raw_md = storage.get_markdown(s3_key) or ""
            clean_paras = _clean_markdown_body_paragraphs(raw_md)
            if clean_paras:
                if len(clean_paras) > 7 and len(dedup_articles) == 1:
                    # Conserve tous les blocs contenant un sous-titre numéroté [1. ...] (condensés à 200 car.) ainsi que le début, le milieu et la conclusion
                    numbered = [p[:200] for p in clean_paras if re.search(r"\[\d+[\.\)]\s+", p)]
                    mid_idx = len(clean_paras) // 2
                    sampled = list(
                        dict.fromkeys(
                            clean_paras[:2]
                            + numbered
                            + clean_paras[mid_idx : mid_idx + 2]
                            + clean_paras[-2:]
                        )
                    )
                    art["excerpt"] = " ".join(sampled)[:2400]
                else:
                    art["excerpt"] = " ".join(clean_paras[:6])[:1400]

    if len(dedup_articles) <= 1:
        if dedup_articles:
            clean_ex = clean_aggregator_noise(dedup_articles[0].get("excerpt") or "")
            clean_ex = " ".join(_clean_markdown_body_paragraphs(clean_ex)) or clean_ex
            needs_llm = (
                len(clean_ex) >= 320
                or os.environ.get("MORNING_REVIEW_FORCE_LLM") == "1"
                or any(
                    m in (topic.summary or "").lower()
                    for m in ("member-only story", "hi everyone", "](http", "• [")
                )
            )
            if needs_llm:
                llm_res = _call_llm_topic_synthesis_and_subtopics(topic.title, dedup_articles)
                if llm_res and llm_res.get("summary"):
                    topic.summary = llm_res["summary"]
                    if llm_res.get("subtopics"):
                        topic.subtopics_json = llm_res["subtopics"]
                else:
                    topic.summary = clean_ex[:750] or clean_aggregator_noise(topic.title)
            elif clean_ex and (" • [" in (topic.summary or "") or not topic.summary or "Source RSS" in (topic.summary or "")):
                topic.summary = clean_ex[:750]
            elif not topic.summary:
                topic.summary = clean_ex[:750] or clean_aggregator_noise(topic.title)
    else:
        llm_res = _call_llm_topic_synthesis_and_subtopics(topic.title, dedup_articles)
        if llm_res and llm_res.get("summary"):
            topic.summary = llm_res["summary"]
            if llm_res.get("subtopics"):
                topic.subtopics_json = llm_res["subtopics"]
        else:
            snippets = [
                f"[{_short_source_label(a['source_title'])}] Titre: {clean_aggregator_noise(a['title'])} | Extrait: {clean_aggregator_noise(a['excerpt'] or '')[:260]}"
                for a in dedup_articles
            ]
            llm_summary = _call_openrouter_synthesis(topic.title, snippets)
            if llm_summary:
                topic.summary = clean_aggregator_noise(llm_summary)[:900]
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
    "suite", "autour", "revient", "explique", "raconte", "présente", "annonce",
    "nouveau", "nouvelle", "nouveaux", "nouvelles", "grand", "grande", "géant",
}


def _clean_text_for_novelty(raw: str) -> str:
    from morning_review.pipeline.ingestion import clean_aggregator_noise

    txt = clean_aggregator_noise(raw or "")
    txt = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", txt)
    txt = re.sub(r"https?://\S+", "", txt)
    txt = re.sub(r"^#+\s+.*$", "", txt, flags=re.MULTILINE)
    txt = re.sub(r"^-\s+\*\*(?:Source|URL|Publié|Lien)[^*]*\*\*.*$", "", txt, flags=re.MULTILINE)
    txt = re.sub(r"\(\d+\s*minute\s*read\)", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _call_llm_novelty_analysis(
    snapshot_text: str,
    *,
    new_title: str,
    new_text: str,
    source_label: str,
    allow_local_ollama: bool = True,
) -> tuple[bool, str] | None:
    """Analyse sémantique par LLM (OpenRouter ou Ollama local) pour déterminer si un nouvel
    article apporte réellement un fait nouveau ou un nouveau sous-sujet par rapport au
    résumé et aux sous-sujets déjà lus par l'utilisateur.
    Retourne `(is_novel, concise_update_summary)` ou `None` si aucun LLM n'est joignable.
    """
    system_prompt = (
        "Tu es un éditeur de veille exigeant chargé d'éviter le bruit (Inbox Zero). "
        "Ta mission est de comparer un sujet DÉJÀ LU (son résumé et ses sous-sujets connus) "
        "avec un nouvel article publié sur le même thème.\n"
        "RÈGLE STRICTE :\n"
        "- Si le nouvel article ne fait que rapporter le même événement initial, le même scandale, "
        "la même panne ou les mêmes faits avec d'autres mots ou par un autre média, tu DOIS répondre `\"is_novel\": false`.\n"
        "- Tu ne dois répondre `\"is_novel\": true` QUE si le nouvel article apporte un rebondissement factuel concret, "
        "une nouvelle étape chronologique (ex: sanction officielle, correctif, démission, rétablissement, post-mortem technique), "
        "un nouveau sous-sujet inédit ou de nouveaux chiffres majeurs absents du résumé déjà lu.\n"
        "Réponds UNIQUEMENT en JSON valide avec les clés :\n"
        "{\"is_novel\": boolean, \"new_subtopic\": \"titre court du sous-sujet inédit ou vide\", "
        "\"update_summary\": \"1 phrase concise et factuelle en français résumant uniquement l'apport nouveau (vide si is_novel=false)\"}"
    )
    user_prompt = (
        f"=== SUJET DÉJÀ LU (RÉSUMÉ & SOUS-SUJETS CONNUS) ===\n{snapshot_text[:2200]}\n\n"
        f"=== NOUVEL ARTICLE ({source_label}) ===\n"
        f"Titre : {new_title}\n"
        f"Extrait : {new_text[:1000]}\n\n"
        "Cet article apporte-t-il un fait réellement nouveau ou un sous-sujet inédit qui n'était pas dans le sujet déjà lu ?"
    )
    res = _call_llm_json(
        system_prompt,
        user_prompt,
        timeout=10.0,
        allow_local_ollama=allow_local_ollama,
    )
    if not isinstance(res, dict) or "is_novel" not in res:
        return None

    is_novel = bool(res.get("is_novel"))
    if not is_novel:
        return False, ""

    from morning_review.pipeline.ingestion import clean_aggregator_noise

    update_summary = clean_aggregator_noise(str(res.get("update_summary") or "")).strip()
    new_subtopic = clean_aggregator_noise(str(res.get("new_subtopic") or "")).strip()
    if len(update_summary) < 15:
        return False, ""
    if new_subtopic and new_subtopic.lower() not in update_summary.lower() and len(new_subtopic) <= 55:
        formatted = f"[{source_label} — {new_subtopic}] {update_summary.rstrip('.')}."
    else:
        formatted = f"[{source_label}] {update_summary.rstrip('.')}."
    return True, formatted[:550]


def evaluate_topic_novelty(
    archived_summary_snapshot: str | None,
    *,
    new_title: str,
    new_text: str,
    source_title: str = "",
    use_llm: bool = True,
) -> tuple[bool, list[str], str]:
    """Vérifie si un nouvel article (`new_title` + `new_text`) apporte des éléments clés inédits
    ou un nouveau sous-sujet qui ne figurent pas déjà dans `archived_summary_snapshot`.
    Combine un pré-filtrage lexical/conceptuel et une analyse sémantique LLM (OpenRouter / Ollama).
    Retourne `(is_novel, novel_points, formatted_update_text)`.
    """
    base_snapshot = _clean_text_for_novelty(archived_summary_snapshot or "")
    src_label = _short_source_label(source_title or "Nouvelle source")

    clean_new_body = _clean_text_for_novelty(new_text or "")
    clean_new_title = _clean_text_for_novelty(new_title or "")
    if not clean_new_body and not clean_new_title:
        return False, [], ""

    old_segments = [
        seg.strip()
        for seg in re.split(r"(?:•|\n|(?<=[.!?])\s+)", base_snapshot)
        if len(seg.strip()) >= 15
    ]
    old_word_sets = [_word_set(seg) for seg in old_segments if _word_set(seg)]
    all_old_words = _word_set(base_snapshot)
    old_numbers = set(re.findall(r"\b\d+(?:[.,]\d+)?%?\b", base_snapshot))

    candidate_sentences: list[str] = []
    for seg in re.split(r"(?:•|\n|(?<=[.!?])\s+)", clean_new_body):
        s_clean = seg.strip(" -•*[]")
        if len(s_clean) >= 28 and s_clean.lower() != clean_new_title.lower():
            candidate_sentences.append(s_clean)
    if clean_new_title and len(clean_new_title) >= 20 and not candidate_sentences:
        candidate_sentences.append(clean_new_title)

    # 1. Filtrage des phrases candidates pour identifier les propositions potentiellement inédites
    novel_points: list[str] = []
    seen_new_sets: list[set[str]] = []

    for cand in candidate_sentences:
        wset = _word_set(cand)
        if not wset:
            continue
        max_old_jaccard = max((_jaccard(wset, ow) for ow in old_word_sets), default=0.0)
        if max_old_jaccard >= 0.36:
            # Simple reformulation d'un élément ou sous-sujet déjà présent dans le résumé archivé
            continue
        if any(_jaccard(wset, prev) >= 0.45 for prev in seen_new_sets):
            continue

        novel_words = {
            w for w in wset
            if len(w) >= 4 and w not in _NOVELTY_STOPWORDS and w not in all_old_words
        }
        cand_numbers = set(re.findall(r"\b\d+(?:[.,]\d+)?%?\b", cand))
        novel_numbers = cand_numbers - old_numbers

        # Exige soit de nouveaux chiffres accompagnés de termes inédits, soit au moins 4 termes significatifs nouveaux
        if (len(novel_numbers) >= 1 and len(novel_words) >= 2) or len(novel_words) >= 4:
            seen_new_sets.append(wset)
            snippet = cand[:240].rstrip(".") + "."
            novel_points.append(snippet)
            if len(novel_points) >= 2:
                break

    if not novel_points:
        return False, [], ""

    # 2. Analyse sémantique LLM (OpenRouter ou Ollama local) pour confirmer qu'il s'agit d'un vrai apport
    # et non d'un simple angle journalistique redondant sur le même fait initial
    if use_llm:
        llm_verdict = _call_llm_novelty_analysis(
            archived_summary_snapshot or base_snapshot,
            new_title=clean_new_title,
            new_text=clean_new_body or clean_new_title,
            source_label=src_label,
        )
        if llm_verdict is not None:
            is_novel_llm, llm_formatted = llm_verdict
            if not is_novel_llm:
                return False, [], ""
            return True, [llm_formatted], llm_formatted

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
    `archived_summary_snapshot` (Résumé + Sous-sujets). Si oui, réactive le sujet
    (`is_archived = False`, `has_updates = True`) et enregistre la mention des nouveautés dans `updates_summary`.
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
        snapshot_ref = state.archived_summary_snapshot or build_topic_structured_snapshot(session, topic)
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
        state.archived_summary_snapshot = f"{snapshot_ref}\n- {formatted_update}"[:4000]
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
    Lors de l'archivage, enregistre le snapshot structuré (Résumé + Sous-sujets) afin de détecter les futures nouveautés.
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
    snapshot = build_topic_structured_snapshot(session, topic, dedup_articles)

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


_BOILERPLATE_PATTERNS: tuple[str, ...] = (
    "subscribe to our newsletter",
    "sign up for our",
    "all rights reserved",
    "tous droits réservés",
    "cookie policy",
    "politique de confidentialité",
    "terms of service",
    "lire aussi :",
    "à lire aussi",
    "partager sur",
    "share on twitter",
    "follow us on",
    "suivez-nous sur",
    "crédit photo",
    "advertising",
    "publicité",
)


def _clean_markdown_body_paragraphs(md_text: str) -> list[str]:
    """Extrait les paragraphes substantiels d'un document Markdown en filtrant les métadonnées d'en-tête,
    les encarts autopromotionnels multi-lignes (ex: '[Entretien ...](url)') et le bruit de navigation.
    """
    if not md_text:
        return []

    # 1. Supprime les encarts autopromotionnels multi-lignes en tête d'article (ex: Pour la Science "Entretien ...", Google News pref)
    cleaned_md = re.sub(
        r"\[\s*(?:Entretien|À lire aussi|Lire aussi|Ajouter [^\]]+ à vos sources)[\s\S]*?\]\(https?://[^)]+\)",
        "",
        md_text,
        flags=re.IGNORECASE,
    )
    # 2. Aplatit les liens Markdown multi-lignes [texte\nsur plusieurs lignes](url) -> texte
    cleaned_md = re.sub(
        r"\[([^\]]+)\]\(https?://[^)]+\)",
        lambda m: " ".join(m.group(1).split()),
        cleaned_md,
    )
    cleaned_md = re.sub(r"^\s*Member-only story\s*$", "", cleaned_md, flags=re.IGNORECASE | re.MULTILINE)

    paragraphs: list[str] = []
    pending_heading = ""
    for raw_block in re.split(r"\n\s*\n", cleaned_md):
        lines: list[str] = []
        for raw_line in raw_block.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line == "---":
                continue
            if line.startswith("#"):
                # Conserve les sous-titres numérotés (ex: "## 1. Terraform") comme repères structurels
                heading_clean = line.lstrip("#").strip()
                if re.match(r"^\d+[\.\)]\s+\S+", heading_clean):
                    pending_heading = f"[{heading_clean}]"
                continue
            if re.match(
                r"^-\s*\*\*(?:Source|Catégorie|URL|Publié|Méthode|Paywall|Lien)[^*]*\*\*\s*:",
                line,
                flags=re.IGNORECASE,
            ):
                continue
            # Nettoie les liens Markdown restants ou tronqués
            line = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", line)
            line = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", line)
            line = re.sub(r"\]\(https?://\S+", "", line)
            line = re.sub(r"https?://\S+", "", line)
            line = re.sub(r"^\s*Member-only story\s*", "", line, flags=re.IGNORECASE)
            line = re.sub(r"^\s*Hi everyone,\s*welcome back\.?\s*", "", line, flags=re.IGNORECASE)
            line = re.sub(r"\(\d+\s*min(?:ute)?s?\s*read\)", "", line, flags=re.IGNORECASE)
            line = re.sub(r"\s+", " ", line).strip()
            if len(line) < 20:
                continue
            low = line.lower()
            if any(bp in low for bp in _BOILERPLATE_PATTERNS):
                continue
            lines.append(line)

        if not lines:
            continue
        para = " ".join(lines).strip()
        if len(para) >= 35:
            if pending_heading:
                para = f"{pending_heading} {para}"
                pending_heading = ""
            paragraphs.append(para)

    return paragraphs


def _score_informative_sentence(
    sentence: str,
    topic_subj: set[str],
    topic_concepts: set[str],
    position_idx: int,
) -> float:
    """Calcule un score de densité informationnelle pour une phrase afin de construire
    un résumé approfondi auto-suffisant (faits, chiffres, mécanismes techniques, conclusions).
    """
    s = sentence.strip()
    length = len(s)
    if length < 40 or length > 420:
        return -1.0

    score = 1.0
    # Bonus de position : les premières phrases posent le fait central, mais on valorise aussi le corps
    if position_idx == 0:
        score += 1.4
    elif position_idx <= 3:
        score += 0.9
    elif position_idx <= 8:
        score += 0.5

    # Longueur idéale d'une phrase explicative complète (80..260 caractères)
    if 80 <= length <= 260:
        score += 0.8
    elif 55 <= length < 80 or 260 < length <= 340:
        score += 0.4

    words = {
        w.lower()
        for w in re.findall(r"[a-zA-ZÀ-ÿ0-9]{3,}", s)
    }
    subj_hits = len(words & {x.lower() for x in topic_subj})
    concept_hits = len(words & {x.lower() for x in topic_concepts})
    score += min(2.0, subj_hits * 0.65 + concept_hits * 0.40)

    # Présence de données chiffrées, pourcentages, versions, montants ou benchmarks
    num_hits = len(re.findall(r"\b\d+(?:[.,]\d+)?(?:\s*%|\s*ms|\s*Go|\s*GB|\s*M|\s*B|\s*Md|\s*€|\s*\$|x)?\b", s))
    score += min(1.5, num_hits * 0.45)

    # Connecteurs analytiques, explicatifs et factuels
    analytical_markers = (
        "permet", "explique", "montre", "révèle", "démontre", "selon", "annonc", "publi",
        "architecture", "modèle", "étude", "chercheur", "résultat", "performance", "sécurité",
        "cependant", "toutefois", "concrètement", "notamment", "contrairement", "objectif",
        "allows", "enables", "demonstrates", "reveals", "according", "announced", "released",
        "researchers", "benchmark", "performance", "however", "specifically", "designed",
        "because", "parce que", "grâce à", "afin de",
    )
    low = s.lower()
    marker_hits = sum(1 for m in analytical_markers if m in low)
    score += min(1.6, marker_hits * 0.4)

    return round(score, 4)


def _call_openrouter_deep_briefing(
    title: str,
    source_blocks: list[str],
) -> dict[str, Any] | None:
    """Si OPENROUTER_API_KEY est configurée, génère un briefing structuré complet (Cœur du sujet + Points clés)."""
    api_key = (settings.openrouter_api_key or "").strip()
    if not api_key or api_key.startswith("sk-or-v1-replace"):
        return None

    joined = "\n\n".join(source_blocks[:6])
    prompt = (
        f"Sujet : {title}\n\n"
        f"Contenus extraits des sources :\n{joined[:6000]}\n\n"
        "Rédige en français une fiche de synthèse approfondie et auto-suffisante permettant de "
        "comprendre intégralement le cœur du sujet sans avoir besoin de cliquer sur les articles sources.\n"
        "Format attendu :\n"
        "COEUR_DU_SUJET:\n"
        "(2 à 3 paragraphes denses expliquant le contexte, le mécanisme/les faits précis, les chiffres et les implications)\n"
        "POINTS_CLES:\n"
        "- [Nom de la source] Point clé factuel détaillé 1\n"
        "- [Nom de la source] Point clé factuel détaillé 2\n"
        "- [Nom de la source] Point clé factuel détaillé 3\n"
        "- [Nom de la source] Point clé factuel détaillé 4\n"
        "- [Nom de la source] Point clé factuel détaillé 5"
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://localhost:8000",
        "X-Title": "Morning Review",
    }
    with httpx.Client(timeout=20.0) as client:
        for model_slug in _get_openrouter_models():
            try:
                resp = client.post(
                    f"{settings.openrouter_base_url.rstrip('/')}/chat/completions",
                    headers=headers,
                    json={
                        "model": model_slug,
                        "messages": [
                            {
                                "role": "system",
                                "content": "Tu es un analyste technique et scientifique rédigeant des briefings exécutifs complets et factuels.",
                            },
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.2,
                        "max_tokens": 900,
                    },
                )
                if resp.status_code == 200:
                    content = (
                        resp.json()
                        .get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")
                        .strip()
                    )
                    if "COEUR_DU_SUJET:" in content and "POINTS_CLES:" in content:
                        parts = content.split("POINTS_CLES:", 1)
                        core_text = parts[0].replace("COEUR_DU_SUJET:", "").strip()
                        bullets_raw = [
                            line.lstrip("-•* ").strip()
                            for line in parts[1].splitlines()
                            if line.strip().startswith(("-", "•", "*"))
                        ]
                        if len(core_text) >= 120 and bullets_raw:
                            return {
                                "core_synthesis": core_text,
                                "key_takeaways": bullets_raw[:8],
                            }
            except Exception as exc:
                logger.debug("OpenRouter deep briefing fallback (%s): %s", model_slug, exc)
    return None


def build_topic_deep_detail(
    session: Session,
    *,
    user_id: UUID,
    topic_id: UUID,
    auto_enrich: bool = True,
) -> dict[str, Any]:
    """Construit la Vue Détail Approfondie ("Zero-Click Briefing") d'un sujet (`topic_id`) :
    1. Si le texte stocké est court (< 700 caractères) et `auto_enrich=True`, déclenche
       automatiquement l'extraction profonde (`enrich_topic_content` via Trafilatura / JSON-LD / MCP).
    2. Agrège les paragraphes de tous les articles rattachés au `topic_id`.
    3. Génère :
       - `core_synthesis` : synthèse narrative approfondie (2 à 3 paragraphes complets expliquant le cœur du sujet).
       - `key_takeaways` : 4 à 8 points clés factuels et chiffrés attribués à chaque source.
       - `sources_breakdown` : liste complète des sources regroupées avec leur angle/extrait détaillé.
       - `full_markdown` : contenu intégral extrait pour lecture approfondie.
    """
    from morning_review.connectors.mcp_enricher import _fetch_via_trafilatura, enrich_topic_content
    from morning_review.db.models import UserTopicFeedback
    from morning_review.pipeline.ingestion import extract_story_profile

    topic = session.get(Topic, topic_id)
    if topic is None or (topic.visibility != "public" and topic.user_id != user_id):
        raise ValueError(f"Topic introuvable : {topic_id}")

    items = session.scalars(
        select(CanonicalItem)
        .where(CanonicalItem.topic_id == topic.id)
        .order_by(CanonicalItem.published_at.desc().nullslast(), CanonicalItem.created_at.desc())
    ).all()

    primary_md = ""
    if items and items[0].s3_key:
        primary_md = storage.get_markdown(items[0].s3_key) or ""
    primary_paras = _clean_markdown_body_paragraphs(primary_md)
    primary_body_chars = sum(len(p) for p in primary_paras)

    needs_mcp_upgrade = (
        "member-only story" in primary_md.lower()
        and "mcp_auth:" not in primary_md
    )
    enrichment_info: dict[str, Any] | None = None
    if (
        auto_enrich
        and (primary_body_chars < 700 or needs_mcp_upgrade)
        and topic.primary_url
        and topic.primary_url.startswith(("http://", "https://"))
    ):
        try:
            enrichment_info = enrich_topic_content(session, user_id=user_id, topic_id=topic.id)
            items = session.scalars(
                select(CanonicalItem)
                .where(CanonicalItem.topic_id == topic.id)
                .order_by(CanonicalItem.published_at.desc().nullslast(), CanonicalItem.created_at.desc())
            ).all()
        except Exception as exc:
            logger.debug("Auto-enrich fallback for topic %s: %s", topic.id, exc)

    dedup_articles = get_deduplicated_topic_articles(session, topic.id)
    topic_subj, topic_concepts = extract_story_profile(topic.title or "", topic.summary or "")

    # Associe chaque URL canonique à ses paragraphes Markdown stockés sur S3
    url_to_paragraphs: dict[str, list[str]] = {}
    all_markdown_sections: list[str] = []
    for it in items:
        raw_md = storage.get_markdown(it.s3_key) if it.s3_key else ""
        paras = _clean_markdown_body_paragraphs(raw_md)
        if not paras and it.excerpt:
            paras = _clean_markdown_body_paragraphs(it.excerpt) or [it.excerpt.strip()]
        if paras:
            url_to_paragraphs[it.canonical_url] = paras
        if raw_md:
            all_markdown_sections.append(raw_md.strip())

    # Si certaines sources secondaires n'ont qu'un court extrait (< 220 car.), tente un fetch rapide sur max 2 sources
    secondary_fetched = 0
    if auto_enrich and len(dedup_articles) > 1:
        for art in dedup_articles[1:4]:
            art_url = art.get("url") or ""
            existing_p = url_to_paragraphs.get(art_url, [])
            if sum(len(p) for p in existing_p) < 220 and art_url.startswith(("http://", "https://")):
                try:
                    fetched = _fetch_via_trafilatura(art_url)
                    f_paras = _clean_markdown_body_paragraphs(fetched.get("markdown") or "")
                    if f_paras:
                        url_to_paragraphs[art_url] = f_paras
                        secondary_fetched += 1
                except Exception:
                    pass
                if secondary_fetched >= 2:
                    break

    # Construction de sources_breakdown et collecte des phrases candidates scorées
    sources_breakdown: list[dict[str, Any]] = []
    scored_sentences: list[tuple[float, int, str, str, set[str]]] = []
    source_blocks_for_llm: list[str] = []

    for src_idx, art in enumerate(dedup_articles):
        art_url = art.get("url") or ""
        src_title = art.get("source_title") or "Source"
        short_lbl = _short_source_label(src_title)
        paras = url_to_paragraphs.get(art_url) or []
        if not paras:
            raw_ex = (art.get("excerpt") or "").strip()
            if raw_ex:
                paras = [re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", raw_ex)]

        # Extrait détaillé spécifique à cette source (jusqu'à 650 caractères)
        detailed_excerpt = " ".join(paras)[:650].strip() if paras else (art.get("title") or "")
        sources_breakdown.append(
            {
                "title": art.get("title") or topic.title,
                "url": art_url,
                "source_title": src_title,
                "short_source": short_lbl,
                "kind": art.get("kind") or "rss",
                "labels": art.get("labels") or [],
                "creator": art.get("creator") or "",
                "published_at": art.get("published_at"),
                "detailed_excerpt": detailed_excerpt,
            }
        )
        source_blocks_for_llm.append(
            f"[{short_lbl}] Titre: {art.get('title')}\nContenu: {' '.join(paras)[:1200]}"
        )

        # Découpe les paragraphes en phrases et les score par densité d'information
        sent_pos = 0
        for p in paras[:12]:
            for raw_s in re.split(r"(?<=[.!?])\s+", p):
                s_clean = raw_s.strip()
                if len(s_clean) < 35:
                    continue
                wset = _word_set(s_clean)
                if len(wset) < 4:
                    continue
                sc = _score_informative_sentence(s_clean, topic_subj, topic_concepts, sent_pos)
                # Léger bonus pour diversifier les sources dès les premiers points
                if src_idx > 0 and sent_pos == 0:
                    sc += 0.45
                if sc > 0:
                    scored_sentences.append((sc, sent_pos + src_idx * 100, short_lbl, s_clean, wset))
                sent_pos += 1

    # Synthèse via OpenRouter si disponible, sinon synthèse extractive haute densité déterministe
    llm_briefing = _call_openrouter_deep_briefing(topic.title, source_blocks_for_llm)

    if llm_briefing:
        core_synthesis = llm_briefing["core_synthesis"]
        key_takeaways = [
            {"source": "Synthèse", "text": b}
            for b in llm_briefing["key_takeaways"]
        ]
    else:
        # Sélectionne les meilleures phrases non redondantes pour les points clés (4 à 7 points)
        scored_sorted = sorted(scored_sentences, key=lambda x: x[0], reverse=True)
        selected_for_takeaways: list[tuple[int, str, str]] = []
        seen_wsets: list[set[str]] = []

        # 1er passage : garantit au moins 1 point fort par source distincte (jusqu'à 5 sources)
        covered_sources: set[str] = set()
        for sc, order_key, lbl, s_text, wset in scored_sorted:
            if lbl in covered_sources:
                continue
            if any(_jaccard(wset, prev) >= 0.42 for prev in seen_wsets):
                continue
            seen_wsets.append(wset)
            covered_sources.add(lbl)
            selected_for_takeaways.append((order_key, lbl, s_text))
            if len(selected_for_takeaways) >= 5:
                break

        # 2e passage : complète avec les phrases les plus denses (chiffres, mécanismes, explications)
        for sc, order_key, lbl, s_text, wset in scored_sorted:
            if len(selected_for_takeaways) >= 7:
                break
            if any(_jaccard(wset, prev) >= 0.42 for prev in seen_wsets):
                continue
            seen_wsets.append(wset)
            selected_for_takeaways.append((order_key, lbl, s_text))

        # Trie les points retenus dans l'ordre logique de lecture pour former le Cœur du sujet
        ordered_points = sorted(selected_for_takeaways, key=lambda x: x[0])
        key_takeaways = [
            {
                "source": lbl,
                "text": s_text if s_text.endswith((".", "!", "?")) else f"{s_text}.",
            }
            for _, lbl, s_text in selected_for_takeaways
        ]

        # Construit `core_synthesis` (2 à 3 paragraphes fluides et substantiels)
        all_clean_paras: list[str] = []
        para_wsets: list[set[str]] = []
        for art in dedup_articles[:5]:
            for p in (url_to_paragraphs.get(art.get("url") or "") or [])[:5]:
                pw = _word_set(p)
                if len(pw) < 6:
                    continue
                if any(_jaccard(pw, prev) >= 0.48 for prev in para_wsets):
                    continue
                para_wsets.append(pw)
                all_clean_paras.append(p)
                if sum(len(x) for x in all_clean_paras) >= 1500 or len(all_clean_paras) >= 4:
                    break
            if sum(len(x) for x in all_clean_paras) >= 1500 or len(all_clean_paras) >= 4:
                break

        if len(all_clean_paras) >= 2:
            core_synthesis = "\n\n".join(all_clean_paras[:3])
        elif ordered_points:
            # Regroupe les phrases denses en 2 paragraphes lisibles
            mid = max(1, len(ordered_points) // 2)
            p1 = " ".join(x[2] for x in ordered_points[:mid])
            p2 = " ".join(x[2] for x in ordered_points[mid:])
            core_synthesis = f"{p1}\n\n{p2}".strip() if p2 else p1
        else:
            core_synthesis = topic.summary or topic.title

    # Si le résumé de carte (`topic.summary`) était très court (< 240 car.) et qu'on a extrait un meilleur cœur de sujet,
    # on enrichit également `topic.summary` pour les prochains affichages dans le flux !
    if core_synthesis and len(topic.summary or "") < 240 and len(core_synthesis) > len(topic.summary or ""):
        first_para = core_synthesis.split("\n\n")[0].strip()
        if len(first_para) >= 80:
            topic.summary = first_para[:700]
            session.commit()

    full_markdown = "\n\n---\n\n".join(all_markdown_sections[:4]) if all_markdown_sections else core_synthesis

    st = session.get(UserTopicState, {"user_id": user_id, "topic_id": topic.id})
    fb = session.get(UserTopicFeedback, {"user_id": user_id, "topic_id": topic.id})
    subtopics = extract_topic_subtopics(session, topic, dedup_articles)

    return {
        "topic_id": str(topic.id),
        "title": topic.title,
        "category": topic.category_slug,
        "primary_url": topic.primary_url,
        "source_count": len(dedup_articles) or topic.source_count,
        "user_vote": fb.vote if fb else 0,
        "is_archived": bool(st.is_archived) if st else False,
        "archived_at": st.archived_at.isoformat() if (st and st.archived_at) else None,
        "reactivated_at": st.reactivated_at.isoformat() if (st and st.reactivated_at) else None,
        "has_updates": bool(st.has_updates) if st else False,
        "updates_summary": st.updates_summary if st else None,
        "summary": topic.summary,
        "subtopics": subtopics,
        "core_synthesis": core_synthesis,
        "key_takeaways": key_takeaways,
        "sources_breakdown": sources_breakdown,
        "full_markdown": full_markdown,
        "enrichment": enrichment_info,
        "first_seen_at": topic.first_seen_at.isoformat() if topic.first_seen_at else None,
        "last_seen_at": topic.last_seen_at.isoformat() if topic.last_seen_at else None,
    }

