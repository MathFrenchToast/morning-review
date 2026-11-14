from __future__ import annotations

import csv
import io
import os
import re
import urllib.parse
from typing import Any
from uuid import UUID

import feedparser
import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from morning_review.connectors.source_registry import (
    export_sources_to_yaml,
    infer_newsletter_category,
    infer_source_labels_and_creator,
)
from morning_review.db.models import (
    CanonicalItem,
    Source,
    SourceMention,
    Topic,
    UserCategory,
    UserSubscription,
)

YOUTUBE_SOCS_COOKIE = {"SOCS": "CAESEwgDEgk0ODE3Nzk3MjQaAmZyIAEaBgiA_LyaBg"}
YOUTUBE_CHANNEL_ID_RE = re.compile(r"\b(UC[a-zA-Z0-9_-]{22})\b")


def _slugify_creator(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-") or "creator"


def upsert_creator_channel_source(
    session: Session,
    *,
    user_id: UUID,
    kind: str,
    title: str,
    url: str,
    site_url: str | None = None,
    category_slug: str = "tech",
    creator: str | None = None,
    extra_labels: list[str] | None = None,
    enabled: bool = True,
    discovery_method: str = "creator_import",
) -> tuple[Source, UserSubscription]:
    """Crée ou met à jour une source (podcast, youtube, rss) avec ses multi-labels et son créateur."""
    clean_url = url.strip()
    clean_title = title.strip() or clean_url
    clean_cat = (category_slug or "tech").strip().lower()

    source = session.scalar(select(Source).where(Source.url == clean_url))
    labels, resolved_creator = infer_source_labels_and_creator(
        kind=kind,
        title=clean_title,
        url=clean_url,
        category_slug=clean_cat,
        existing_meta=source.metadata_json if source else None,
        explicit_labels=extra_labels,
        explicit_creator=creator,
    )
    meta = {
        **((source.metadata_json if source else None) or {}),
        "labels": labels,
        "creator": resolved_creator,
    }

    if source is None:
        source = Source(
            kind=kind,
            url=clean_url,
            site_url=site_url,
            title=clean_title[:255],
            is_public=True,
            metadata_json=meta,
        )
        session.add(source)
        session.flush()
    else:
        source.kind = kind
        source.title = clean_title[:255]
        if site_url:
            source.site_url = site_url
        source.metadata_json = meta

    cat_obj = session.scalar(
        select(UserCategory).where(
            UserCategory.user_id == user_id,
            UserCategory.slug == clean_cat,
        )
    )
    sub = session.scalar(
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
            category_slug=clean_cat,
            enabled=enabled,
            discovery_method=discovery_method,
        )
        session.add(sub)
        session.flush()
    else:
        sub.category_slug = clean_cat
        if cat_obj:
            sub.category_id = cat_obj.id
        sub.enabled = enabled

    return source, sub


def resolve_youtube_channel_to_feed(
    identifier: str,
    client: httpx.Client | None = None,
) -> dict[str, str] | None:
    """Résout un identifiant YouTube (`UC...`, `@handle`, URL de chaîne ou nom de créateur)
    vers son flux Atom officiel `https://www.youtube.com/feeds/videos.xml?channel_id=UC...`.
    """
    raw = (identifier or "").strip()
    if not raw:
        return None

    own_client = client is None
    http_client = client or httpx.Client(
        timeout=10.0,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) MorningReview/1.0"},
        cookies=YOUTUBE_SOCS_COOKIE,
    )
    try:
        channel_id: str | None = None
        m_direct = YOUTUBE_CHANNEL_ID_RE.search(raw)
        if m_direct:
            channel_id = m_direct.group(1)

        if not channel_id:
            if raw.startswith("http") and "youtube.com/" in raw:
                resp = http_client.get(raw)
                m = (
                    re.search(r'"externalId":"(UC[a-zA-Z0-9_-]{22})"', resp.text)
                    or re.search(r"youtube\.com/channel/(UC[a-zA-Z0-9_-]{22})", resp.text)
                    or re.search(r"channel_id=(UC[a-zA-Z0-9_-]{22})", resp.text)
                )
                if m:
                    channel_id = m.group(1)
            elif raw.startswith("@"):
                resp = http_client.get(f"https://www.youtube.com/{raw}")
                m = (
                    re.search(r'"externalId":"(UC[a-zA-Z0-9_-]{22})"', resp.text)
                    or re.search(r"youtube\.com/channel/(UC[a-zA-Z0-9_-]{22})", resp.text)
                    or re.search(r"channel_id=(UC[a-zA-Z0-9_-]{22})", resp.text)
                )
                if m:
                    channel_id = m.group(1)

        if not channel_id:
            q = urllib.parse.quote(raw.lstrip("@"))
            resp = http_client.get(f"https://www.youtube.com/results?search_query={q}&sp=EgIQAg%3D%3D")
            m = re.search(r'"channelId":"(UC[a-zA-Z0-9_-]{22})"', resp.text)
            if m:
                channel_id = m.group(1)

        if not channel_id:
            return None

        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        site_url = f"https://www.youtube.com/channel/{channel_id}"
        feed_resp = http_client.get(feed_url)
        parsed = feedparser.parse(feed_resp.text)
        feed_title = (parsed.feed.get("title") if parsed and parsed.feed else None) or raw.lstrip("@")
        return {
            "channel_id": channel_id,
            "title": f"{feed_title} (YouTube)" if "youtube" not in feed_title.lower() else feed_title,
            "creator": feed_title,
            "feed_url": feed_url,
            "site_url": site_url,
        }
    except Exception:
        return None
    finally:
        if own_client:
            http_client.close()


def sync_youtube_subscriptions_oauth(
    session: Session,
    user_id: UUID,
    *,
    oauth_token: str | None = None,
    default_category: str = "tech",
    enabled: bool = True,
) -> dict[str, Any]:
    """Interroge l'API YouTube Data v3 (`GET /youtube/v3/subscriptions?part=snippet&mine=true`)
    à l'aide d'un token OAuth 2.0 (`youtube.readonly`), convertit chaque chaîne abonnée en flux
    Atom `https://www.youtube.com/feeds/videos.xml?channel_id=UC...` et l'associe à son créateur.
    """
    token = (oauth_token or os.getenv("YOUTUBE_OAUTH_TOKEN", "")).strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        raise ValueError(
            "Aucun token OAuth YouTube fourni (passez `oauth_token` ou renseignez `YOUTUBE_OAUTH_TOKEN` dans `.env`)."
        )

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    page_token: str | None = None
    channels_synced = 0
    sample_channels: list[dict[str, Any]] = []

    with httpx.Client(timeout=15.0, headers=headers) as client:
        for _ in range(20):
            params: dict[str, Any] = {
                "part": "snippet",
                "mine": "true",
                "maxResults": 50,
            }
            if page_token:
                params["pageToken"] = page_token
            resp = client.get("https://www.googleapis.com/youtube/v3/subscriptions", params=params)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Erreur API YouTube Data v3 (HTTP {resp.status_code}) : {resp.text[:300]}"
                )
            payload = resp.json()
            for item in payload.get("items", []) or []:
                snippet = item.get("snippet", {}) or {}
                res_id = snippet.get("resourceId", {}) or {}
                channel_id = res_id.get("channelId")
                ch_title = (snippet.get("title") or "").strip()
                ch_desc = (snippet.get("description") or "").strip()
                if not channel_id or not ch_title:
                    continue
                feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
                site_url = f"https://www.youtube.com/channel/{channel_id}"
                inferred_cat = infer_newsletter_category("", ch_title, ch_desc)
                cat_slug = inferred_cat if inferred_cat != "perso" else default_category

                src, _ = upsert_creator_channel_source(
                    session,
                    user_id=user_id,
                    kind="youtube",
                    title=f"{ch_title} (YouTube)" if "youtube" not in ch_title.lower() else ch_title,
                    url=feed_url,
                    site_url=site_url,
                    category_slug=cat_slug,
                    creator=ch_title,
                    extra_labels=["youtube", cat_slug],
                    enabled=enabled,
                    discovery_method="youtube_oauth",
                )
                channels_synced += 1
                if len(sample_channels) < 15:
                    sample_channels.append(
                        {
                            "title": src.title,
                            "channel_id": channel_id,
                            "feed_url": feed_url,
                            "creator": (src.metadata_json or {}).get("creator"),
                            "labels": (src.metadata_json or {}).get("labels"),
                        }
                    )

            page_token = payload.get("nextPageToken")
            if not page_token:
                break

    session.commit()
    export_sources_to_yaml(session, user_id=user_id)
    return {
        "channels_synced": channels_synced,
        "sample_channels": sample_channels,
    }


def parse_youtube_feed_channels_paste(text_block: str) -> list[dict[str, str]]:
    """Extrait les chaînes YouTube depuis un copier-coller brut de https://www.youtube.com/feed/channels
    (blocs séparés par 'Abonné' ou 'Subscribed', avec ligne '@handle•X abonnés').
    """
    blocks = re.split(r"\n(?:Abonné|Subscribed)\s*(?:\n|$)", text_block.strip())
    parsed: list[dict[str, str]] = []
    for raw_block in blocks:
        b_lines = [ln.strip() for ln in raw_block.splitlines() if ln.strip()]
        if not b_lines:
            continue
        title = b_lines[0]
        handle = ""
        desc_lines: list[str] = []
        for ln in b_lines[1:]:
            if ln.startswith("@") and "•" in ln:
                handle = ln.split("•", 1)[0].strip()
            elif re.match(r"^[\d\s,\.]+\s*(?:k|M)?\s*(?:d['’])?abonnés", ln, flags=re.IGNORECASE):
                continue
            else:
                desc_lines.append(ln)
        parsed.append(
            {
                "title": title,
                "handle": handle,
                "query": handle or title,
                "description": " ".join(desc_lines),
            }
        )
    return parsed


def import_youtube_channels_or_csv(
    session: Session,
    user_id: UUID,
    *,
    raw_input: str,
    category_slug: str = "tech",
    creator: str | None = None,
    enabled: bool = True,
) -> dict[str, Any]:
    """Importe une ou plusieurs chaînes YouTube à partir :
    - d'un export CSV Google Takeout (`Channel Id,Channel Url,Channel Title`)
    - ou d'une liste de `@handles`, d'URLs de chaînes ou de noms de créateurs.
    """
    text_block = (raw_input or "").strip()
    if not text_block:
        return {"imported_count": 0, "channels": [], "errors": []}

    imported: list[dict[str, Any]] = []
    errors: list[str] = []

    # Cas 1 : Détection d'un CSV Google Takeout (contient des lignes avec UC[22 chars] et virgules)
    lines = [ln.strip() for ln in text_block.splitlines() if ln.strip()]
    is_csv = len(lines) >= 2 and ("," in lines[0]) and any(YOUTUBE_CHANNEL_ID_RE.search(ln) for ln in lines[1:5])

    if is_csv:
        reader = csv.reader(io.StringIO(text_block))
        for row in reader:
            if not row:
                continue
            row_joined = " ".join(row)
            m = YOUTUBE_CHANNEL_ID_RE.search(row_joined)
            if not m:
                continue
            cid = m.group(1)
            ch_title = row[-1].strip() if len(row) >= 3 else cid
            feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={cid}"
            site_url = f"https://www.youtube.com/channel/{cid}"
            cat = infer_newsletter_category("", ch_title, "")
            if cat == "perso" and category_slug:
                cat = category_slug
            src, _ = upsert_creator_channel_source(
                session,
                user_id=user_id,
                kind="youtube",
                title=f"{ch_title} (YouTube)" if "youtube" not in ch_title.lower() else ch_title,
                url=feed_url,
                site_url=site_url,
                category_slug=cat,
                creator=creator or ch_title,
                extra_labels=["youtube", cat],
                enabled=enabled,
                discovery_method="youtube_takeout_csv",
            )
            imported.append(
                {
                    "title": src.title,
                    "feed_url": src.url,
                    "creator": (src.metadata_json or {}).get("creator"),
                    "labels": (src.metadata_json or {}).get("labels"),
                }
            )
    else:
        is_feed_paste = bool(re.search(r"\n(?:Abonné|Subscribed)\s*(?:\n|$)", text_block))
        if is_feed_paste:
            paste_items = parse_youtube_feed_channels_paste(text_block)
            tokens = [item["query"] for item in paste_items if item.get("query")]
        else:
            tokens = [
                tok.strip()
                for tok in re.split(r"[\n,;]+", text_block)
                if tok.strip()
            ]
        with httpx.Client(
            timeout=10.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) MorningReview/1.0"},
            cookies=YOUTUBE_SOCS_COOKIE,
        ) as client:
            for tok in tokens:
                resolved = resolve_youtube_channel_to_feed(tok, client=client)
                if not resolved:
                    errors.append(f"Impossible de résoudre la chaîne YouTube : {tok}")
                    continue
                src, _ = upsert_creator_channel_source(
                    session,
                    user_id=user_id,
                    kind="youtube",
                    title=resolved["title"],
                    url=resolved["feed_url"],
                    site_url=resolved["site_url"],
                    category_slug=category_slug,
                    creator=creator or resolved["creator"],
                    extra_labels=["youtube", category_slug],
                    enabled=enabled,
                    discovery_method="youtube_handle",
                )
                imported.append(
                    {
                        "title": src.title,
                        "feed_url": src.url,
                        "creator": (src.metadata_json or {}).get("creator"),
                        "labels": (src.metadata_json or {}).get("labels"),
                    }
                )

    session.commit()
    export_sources_to_yaml(session, user_id=user_id)
    return {
        "imported_count": len(imported),
        "channels": imported,
        "errors": errors,
    }


def resolve_and_add_podcast(
    session: Session,
    user_id: UUID,
    *,
    query_or_url: str,
    category_slug: str = "tech",
    creator: str | None = None,
    enabled: bool = True,
) -> dict[str, Any]:
    """Ajoute un podcast soit par son URL RSS directe, soit en résolvant son nom via l'API Apple iTunes Search."""
    q_clean = (query_or_url or "").strip()
    if not q_clean:
        raise ValueError("Nom ou URL du podcast requis")

    feed_url = ""
    matched_title = q_clean
    site_url = None

    if q_clean.startswith(("http://", "https://")):
        feed_url = q_clean
    else:
        with httpx.Client(timeout=10.0) as client:
            for country in ("FR", "US"):
                q_enc = urllib.parse.quote(q_clean)
                r = client.get(
                    f"https://itunes.apple.com/search?media=podcast&entity=podcast&country={country}&limit=5&term={q_enc}"
                )
                results = r.json().get("results", []) if r.status_code == 200 else []
                for item in results:
                    if item.get("feedUrl"):
                        feed_url = item["feedUrl"]
                        matched_title = item.get("collectionName") or q_clean
                        site_url = item.get("collectionViewUrl")
                        if not creator:
                            creator = item.get("artistName") or matched_title
                        break
                if feed_url:
                    break

    if not feed_url:
        raise ValueError(f"Aucun flux RSS de podcast trouvé pour '{q_clean}'")

    src, sub = upsert_creator_channel_source(
        session,
        user_id=user_id,
        kind="podcast",
        title=matched_title,
        url=feed_url,
        site_url=site_url,
        category_slug=category_slug,
        creator=creator or matched_title,
        extra_labels=["podcast", category_slug],
        enabled=enabled,
        discovery_method="podcast_search",
    )
    session.commit()
    export_sources_to_yaml(session, user_id=user_id)
    return {
        "source_id": str(src.id),
        "subscription_id": str(sub.id),
        "title": src.title,
        "feed_url": src.url,
        "creator": (src.metadata_json or {}).get("creator"),
        "labels": (src.metadata_json or {}).get("labels"),
        "enabled": sub.enabled,
    }


def list_creators_aggregated(
    session: Session,
    user_id: UUID,
    *,
    only_creator_media: bool = False,
) -> list[dict[str, Any]]:
    """Regroupe les sources par Créateur de contenu (`creator`), agrège leurs différents
    canaux (Podcast, YouTube, RSS, Newsletter), fusionne leurs multi-labels et remonte
    leurs derniers sujets/épisodes dédoublonnés.
    """
    rows = session.execute(
        select(Source, UserSubscription)
        .join(UserSubscription, UserSubscription.source_id == Source.id)
        .where(UserSubscription.user_id == user_id)
        .order_by(Source.title)
    ).all()

    by_creator: dict[str, dict[str, Any]] = {}
    source_to_creator: dict[UUID, str] = {}

    for src, sub in rows:
        labels, creator = infer_source_labels_and_creator(
            kind=src.kind,
            title=src.title,
            url=src.url,
            category_slug=sub.category_slug,
            existing_meta=src.metadata_json,
        )
        source_to_creator[src.id] = creator
        bucket = by_creator.get(creator)
        if bucket is None:
            bucket = {
                "creator": creator,
                "creator_slug": _slugify_creator(creator),
                "labels": [],
                "categories": [],
                "media_types": [],
                "enabled_channels_count": 0,
                "total_channels_count": 0,
                "total_items_ingested": 0,
                "has_podcast_or_youtube": False,
                "channels": [],
                "source_ids": set(),
            }
            by_creator[creator] = bucket

        for lb in labels:
            if lb not in bucket["labels"]:
                bucket["labels"].append(lb)
        if sub.category_slug not in bucket["categories"]:
            bucket["categories"].append(sub.category_slug)

        media_type = (
            "youtube"
            if ("youtube" in labels or src.kind == "youtube")
            else "podcast"
            if ("podcast" in labels or src.kind == "podcast")
            else src.kind
        )
        if media_type not in bucket["media_types"]:
            bucket["media_types"].append(media_type)
        if media_type in ("podcast", "youtube"):
            bucket["has_podcast_or_youtube"] = True

        bucket["total_channels_count"] += 1
        if sub.enabled:
            bucket["enabled_channels_count"] += 1
        bucket["total_items_ingested"] += src.item_count or 0
        bucket["source_ids"].add(src.id)
        bucket["channels"].append(
            {
                "source_id": str(src.id),
                "title": src.title,
                "kind": src.kind,
                "media_type": media_type,
                "url": src.url,
                "site_url": src.site_url,
                "category": sub.category_slug,
                "labels": labels,
                "enabled": sub.enabled,
                "item_count": src.item_count or 0,
                "last_fetched_at": src.last_fetched_at.isoformat() if src.last_fetched_at else None,
            }
        )

    # Récupère les derniers sujets (topics) associés aux canaux de chaque créateur (agrégés par topic_id)
    topic_rows = session.execute(
        select(
            SourceMention.source_id,
            Topic.id,
            Topic.title,
            Topic.primary_url,
            Topic.category_slug,
            Topic.last_seen_at,
            Topic.source_count,
            Source.title,
            Source.kind,
        )
        .join(CanonicalItem, CanonicalItem.id == SourceMention.item_id)
        .join(Topic, Topic.id == CanonicalItem.topic_id)
        .join(Source, Source.id == SourceMention.source_id)
        .order_by(Topic.last_seen_at.desc())
        .limit(800)
    ).all()

    topics_by_creator: dict[str, dict[UUID, dict[str, Any]]] = {}
    for src_id, tid, t_title, t_url, t_cat, t_last, t_scount, src_title, src_kind in topic_rows:
        creator_name = source_to_creator.get(src_id)
        if not creator_name:
            continue
        c_topics = topics_by_creator.setdefault(creator_name, {})
        entry = c_topics.get(tid)
        if entry is None:
            if len(c_topics) >= 8:
                continue
            c_topics[tid] = {
                "topic_id": str(tid),
                "title": t_title,
                "url": t_url,
                "primary_url": t_url,
                "category": t_cat,
                "last_seen_at": t_last.isoformat() if t_last else None,
                "source_count": t_scount,
                "channels_matched": [src_title],
                "channel_kinds": [src_kind],
            }
        else:
            if src_title not in entry["channels_matched"]:
                entry["channels_matched"].append(src_title)
            if src_kind not in entry["channel_kinds"]:
                entry["channel_kinds"].append(src_kind)

    result: list[dict[str, Any]] = []
    for creator_name, bucket in by_creator.items():
        if only_creator_media and not (
            bucket["has_podcast_or_youtube"] or bucket["total_channels_count"] >= 2
        ):
            continue
        bucket.pop("source_ids", None)
        recent_list = list((topics_by_creator.get(creator_name) or {}).values())
        bucket["creator_name"] = bucket["creator"]
        bucket["channels_count"] = bucket["total_channels_count"]
        bucket["enabled_count"] = bucket["enabled_channels_count"]
        bucket["all_enabled"] = (
            bucket["enabled_channels_count"] == bucket["total_channels_count"]
            and bucket["total_channels_count"] > 0
        )
        bucket["enabled"] = bucket["enabled_channels_count"] > 0
        bucket["recent_topics"] = recent_list
        bucket["topics_count"] = len(recent_list)
        result.append(bucket)

    # Trie d'abord les créateurs ayant Podcast/YouTube ou multi-canaux, puis par ordre alphabétique
    result.sort(
        key=lambda x: (
            0 if x["has_podcast_or_youtube"] else 1,
            0 if x["enabled"] else 1,
            -x["total_channels_count"],
            x["creator"].lower(),
        )
    )
    return result
