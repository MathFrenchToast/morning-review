from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from uuid import UUID

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from morning_review.core.config import settings
from morning_review.db.models import Source, UserCategory, UserSubscription, utcnow


def load_yaml_config(yaml_path: Path | None = None) -> dict[str, Any]:
    path = yaml_path or settings.sources_yaml_path
    if not path.exists():
        return {"version": "1.0", "categories": [], "sources": [], "newsletter_rules": {}}
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def infer_newsletter_category(
    sender_email: str,
    title: str,
    subject: str = "",
    yaml_cfg: dict[str, Any] | None = None,
) -> str:
    cfg = yaml_cfg or load_yaml_config()
    rules = cfg.get("newsletter_rules", {}) or {}
    domain_map: dict[str, str] = {
        k.lower(): v for k, v in (rules.get("domain_category_map") or {}).items()
    }
    email_lower = (sender_email or "").lower().strip()
    if email_lower in domain_map:
        return domain_map[email_lower]

    domain = email_lower.split("@")[-1] if "@" in email_lower else email_lower
    for mapped_dom, cat in domain_map.items():
        if domain == mapped_dom or domain.endswith("." + mapped_dom):
            return cat

    combined = f"{title} {subject} {email_lower}".lower()
    if any(k in combined for k in ("science", "scientist", "nature", "cnrs", "physique", "astronomie")):
        return "science"
    if any(k in combined for k in ("canardpc", "jeuxvideo", "hardware", "gaming", "steam", "jeu")):
        return "gaming-hardware"
    if any(
        k in combined
        for k in (
            "tech",
            "devops",
            "kube",
            "cloud",
            "ai",
            "llm",
            "python",
            "rust",
            "golang",
            "tldr",
            "bytebytego",
            "medium",
            "programming",
            "engineering",
            "cyber",
            "security",
        )
    ):
        return "tech"
    return rules.get("default_category", "perso")


CREATOR_NORMALIZATION_RULES: list[tuple[tuple[str, ...], str]] = [
    (("underscore_", "micode"), "Micode / Underscore_"),
    (("hugodécrypte", "hugodecrypte", "hugo décrypte"), "HugoDécrypte"),
    (("lex fridman", "lexfridman"), "Lex Fridman"),
    (("dwarkesh",), "Dwarkesh Patel"),
    (("silicon carne", "carlos diaz", "siliconcarnepod"), "Carlos Diaz / Silicon Carne"),
    (("à la french", "a la french", "alafrench", "mehdi medjaoui"), "À la French"),
    (("platform engineering",), "Platform Engineering"),
    (("bytebytego", "alex xu"), "ByteByteGo"),
    (("hugging face", "huggingface"), "Hugging Face"),
    (("cncf", "cloud native computing foundation"), "CNCF"),
    (("bruno heubi", "figures de sport"), "Bruno Heubi"),
    (("bernth",), "BERNTH"),
    (("quanta", "joy of why"), "Quanta Magazine"),
    (("collège de france", "college de france", "xavier leroy", "antoine georges"), "Collège de France"),
    (("france culture", "science cqfd", "science, cqfd", "avec philosophie"), "France Culture"),
    (("france inter", "le code a changé", "journal de 19h"), "France Inter"),
    (("sismique", "julien devaureix"), "Julien Devaureix (Sismique)"),
    (("le monde fantastique de danou",), "Le Monde Fantastique De Danou"),
    (("lemonde.fr", "le monde -", "(le monde)", "heure du monde", "chaleur humaine"), "Le Monde"),
    (("canard pc", "canardpc"), "Canard PC"),
    (("jeuxvideo.com", "jv.com"), "JeuxVideo.com"),
    (("new scientist", "world, the universe and us"), "New Scientist"),
    (("pour la science", "écoute de la science", "ecoute de la science"), "Pour la Science"),
    (("bbc", "global news podcast", "learning english"), "BBC"),
    (("radio france", "franceinfo"), "Radio France / franceinfo"),
    (("tldr",), "TLDR"),
    (("medium daily digest", "noreply@medium.com"), "Medium Daily Digest"),
    (("next.ink", "entre la chaise et le clavier"), "Next.ink"),
    (("cafzone", "cafzone.net"), "Cafzone"),
    (("geekzone", "torréfaction", "torrefaction"), "Geekzone"),
    (("clever cloud", "message à caractère informatique"), "Clever Cloud"),
    (("404 media",), "404 Media"),
    (("wired", "uncanny valley"), "WIRED"),
]


def infer_source_labels_and_creator(
    *,
    kind: str,
    title: str,
    url: str,
    category_slug: str,
    existing_meta: dict[str, Any] | None = None,
    explicit_labels: list[str] | None = None,
    explicit_creator: str | None = None,
) -> tuple[list[str], str]:
    """Calcule la liste normalisée des labels (ex: ['podcast', 'tech']) et le nom canonique du créateur."""
    meta = existing_meta or {}
    u_low = (url or "").lower()
    t_low = (title or "").lower()
    k_low = (kind or "rss").lower()
    cat_low = (category_slug or "tech").lower()

    raw_labels: list[str] = []
    if explicit_labels:
        raw_labels.extend(str(x).strip().lower() for x in explicit_labels if str(x).strip())
    elif isinstance(meta.get("labels"), list):
        raw_labels.extend(str(x).strip().lower() for x in meta["labels"] if str(x).strip())

    # Détection automatique du format média
    is_youtube = k_low == "youtube" or "youtube.com/feeds/videos.xml" in u_low or "youtube.com/@" in u_low
    is_podcast = (
        k_low == "podcast"
        or "podcast" in raw_labels
        or any(
            h in u_low
            for h in (
                "feeds.acast.com",
                "feeds.megaphone.fm",
                "feed.ausha.co",
                "feeds.audiomeans.fr",
                "anchor.fm/",
                "feeds.simplecast.com",
                "rss.buzzsprout.com",
                "rss.libsyn.com",
                "interactive.libsyn.com",
                "feed.podbean.com",
                "podcasts.files.bbci.co.uk",
                "radiofrance-podcast.net",
                "podcast.college-de-france.fr",
                "feeds.redcircle.com",
                "feeds.soundcloud.com",
                "feeds.podcastics.com",
                "podcastics.com/podcast",
                "rss.art19.com",
                "api.riverside.com/hosting",
                "lepodcast.fr",
                "lexfridman.com/feed/podcast",
                "dwarkesh-podcast",
                "quantapodcast.",
                "joy.quantamagazine.org",
                "next.ink/feed/mp3",
                "geekzone.fr/feed/torrefaction",
            )
        )
    )

    if is_youtube and "youtube" not in raw_labels:
        raw_labels.insert(0, "youtube")
    if is_podcast and "podcast" not in raw_labels:
        raw_labels.insert(0, "podcast")
    if k_low == "newsletter" and "newsletter" not in raw_labels:
        raw_labels.append("newsletter")
    if cat_low and cat_low not in raw_labels:
        raw_labels.append(cat_low)

    dedup_labels = list(dict.fromkeys(raw_labels))

    creator = (explicit_creator or meta.get("creator") or "").strip()
    combined = f"{t_low} {u_low} {creator.lower()}"
    for needles, canonical_creator in CREATOR_NORMALIZATION_RULES:
        if any(n in combined for n in needles):
            creator = canonical_creator
            break

    if not creator:
        # Nettoie le titre pour en déduire un nom de créateur lisible
        cleaned = re.sub(r"\s*\([^)]*\)\s*$", "", title or "").strip()
        cleaned = re.split(r"\s+[—–-]\s+", cleaned)[0].strip()
        creator = cleaned or title

    return dedup_labels, creator


def sync_sources_from_yaml(
    session: Session,
    user_id: UUID,
    yaml_path: Path | None = None,
) -> dict[str, int]:
    """Synchronise les catégories et les flux déclarés dans config/sources.yaml vers PostgreSQL."""
    cfg = load_yaml_config(yaml_path)
    cat_count = 0
    src_count = 0

    cat_by_slug: dict[str, UserCategory] = {
        c.slug: c
        for c in session.scalars(
            select(UserCategory).where(UserCategory.user_id == user_id)
        ).all()
    }

    for cat_def in cfg.get("categories", []) or []:
        slug = str(cat_def["slug"]).strip().lower()
        cat = cat_by_slug.get(slug)
        if cat is None:
            cat = UserCategory(
                user_id=user_id,
                slug=slug,
                name=cat_def.get("name", slug.title()),
                description=cat_def.get("description"),
            )
            session.add(cat)
            session.flush()
            cat_by_slug[slug] = cat
        else:
            cat.name = cat_def.get("name", cat.name)
            cat.description = cat_def.get("description", cat.description)
        cat_count += 1

    for src_def in cfg.get("sources", []) or []:
        url = str(src_def["url"]).strip()
        kind = str(src_def.get("kind", "rss")).strip()
        title = str(src_def.get("title", url)).strip()
        cat_slug = str(src_def.get("category", "tech")).strip().lower()
        is_public = bool(src_def.get("is_public", kind in ("rss", "web", "podcast", "youtube")))
        enabled = bool(src_def.get("enabled", True))

        source = session.scalar(select(Source).where(Source.url == url))
        labels, creator = infer_source_labels_and_creator(
            kind=kind,
            title=title,
            url=url,
            category_slug=cat_slug,
            existing_meta=source.metadata_json if source else None,
            explicit_labels=src_def.get("labels"),
            explicit_creator=src_def.get("creator"),
        )
        new_meta = {
            **((source.metadata_json if source else None) or {}),
            "labels": labels,
            "creator": creator,
        }

        if source is None:
            source = Source(
                kind=kind,
                url=url,
                site_url=src_def.get("site_url"),
                title=title,
                sender_email=src_def.get("sender_email"),
                list_id=src_def.get("list_id"),
                is_public=is_public,
                metadata_json=new_meta,
            )
            session.add(source)
            session.flush()
        else:
            source.kind = kind
            source.title = title
            source.site_url = src_def.get("site_url", source.site_url)
            source.metadata_json = new_meta

        sub = session.scalar(
            select(UserSubscription).where(
                UserSubscription.user_id == user_id,
                UserSubscription.source_id == source.id,
            )
        )
        cat_obj = cat_by_slug.get(cat_slug)
        if sub is None:
            sub = UserSubscription(
                user_id=user_id,
                source_id=source.id,
                category_id=cat_obj.id if cat_obj else None,
                category_slug=cat_slug,
                enabled=enabled,
                discovery_method=src_def.get("discovery_method", "yaml_seed"),
            )
            session.add(sub)
        else:
            sub.category_slug = cat_slug
            sub.category_id = cat_obj.id if cat_obj else sub.category_id
            sub.enabled = enabled
        src_count += 1

    session.commit()
    return {"categories_synced": cat_count, "sources_synced": src_count}


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").lower()).strip("-")
    return slug or "default"


def register_discovered_newsletter(
    session: Session,
    user_id: UUID,
    sender_name: str,
    sender_email: str,
    list_id: str | None = None,
    subject_hint: str = "",
) -> tuple[Source, UserSubscription]:
    """Inscrit ou met à jour automatiquement un flux Newsletter découvert dans Gmail."""
    clean_email = (sender_email or "unknown@newsletter.local").strip().lower()
    clean_list_id = re.sub(r"[<>\"'\s]", "", list_id).lower() if list_id else None
    clean_name = (sender_name or clean_email.split("@")[0]).strip()
    # Normalise "Dan from TLDR" -> "TLDR" pour ne pas créer un doublon avec "TLDR"
    if clean_name.lower().startswith("dan from "):
        clean_name = clean_name[9:].strip()

    name_slug = _slugify(clean_name)
    canonical_key = clean_list_id or f"{clean_email}/{name_slug}"
    url = f"newsletter://{canonical_key}"
    display_title = clean_name
    if display_title and clean_email not in display_title.lower():
        display_title = f"{display_title} ({clean_email})"

    source = session.scalar(select(Source).where(Source.url == url))

    if source is None:
        source = Source(
            kind="newsletter",
            url=url,
            title=display_title[:255],
            sender_email=clean_email,
            list_id=clean_list_id,
            is_public=False,
            last_fetched_at=utcnow(),
            item_count=0,
        )
        session.add(source)
        session.flush()
    else:
        source.last_fetched_at = utcnow()
        if clean_list_id and not source.list_id:
            source.list_id = clean_list_id

    sub = session.scalar(
        select(UserSubscription).where(
            UserSubscription.user_id == user_id,
            UserSubscription.source_id == source.id,
        )
    )
    if sub is None:
        cat_slug = infer_newsletter_category(clean_email, display_title, subject_hint)
        cat_obj = session.scalar(
            select(UserCategory).where(
                UserCategory.user_id == user_id,
                UserCategory.slug == cat_slug,
            )
        )
        sub = UserSubscription(
            user_id=user_id,
            source_id=source.id,
            category_id=cat_obj.id if cat_obj else None,
            category_slug=cat_slug,
            enabled=True,
            discovery_method="gmail_auto",
        )
        session.add(sub)
        session.flush()

    return source, sub


def export_sources_to_yaml(
    session: Session,
    user_id: UUID,
    yaml_path: Path | None = None,
) -> Path:
    """Exporte la liste complète des catégories et flux (incluant les newsletters découvertes) dans config/sources.yaml."""
    path = yaml_path or settings.sources_yaml_path
    existing = load_yaml_config(path)

    categories = session.scalars(
        select(UserCategory)
        .where(UserCategory.user_id == user_id)
        .order_by(UserCategory.slug)
    ).all()

    rows = session.execute(
        select(Source, UserSubscription)
        .join(UserSubscription, UserSubscription.source_id == Source.id)
        .where(UserSubscription.user_id == user_id)
        .order_by(Source.kind, Source.title)
    ).all()

    exported_sources: list[dict[str, Any]] = []
    for source, sub in rows:
        labels, creator = infer_source_labels_and_creator(
            kind=source.kind,
            title=source.title,
            url=source.url,
            category_slug=sub.category_slug,
            existing_meta=source.metadata_json,
        )
        entry: dict[str, Any] = {
            "kind": source.kind,
            "title": source.title,
            "url": source.url,
            "category": sub.category_slug,
            "labels": labels,
            "creator": creator,
            "is_public": source.is_public,
            "enabled": sub.enabled,
            "discovery_method": sub.discovery_method,
        }
        if source.site_url:
            entry["site_url"] = source.site_url
        if source.sender_email:
            entry["sender_email"] = source.sender_email
        if source.list_id:
            entry["list_id"] = source.list_id
        exported_sources.append(entry)

    payload = {
        "version": existing.get("version", "1.0"),
        "categories": [
            {
                "slug": c.slug,
                "name": c.name,
                "description": c.description or "",
            }
            for c in categories
        ],
        "sources": exported_sources,
        "newsletter_rules": existing.get("newsletter_rules", {}),
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(payload, fh, allow_unicode=True, sort_keys=False, width=110)
    return path


def list_user_sources(session: Session, user_id: UUID) -> list[dict[str, Any]]:
    rows = session.execute(
        select(Source, UserSubscription)
        .join(UserSubscription, UserSubscription.source_id == Source.id)
        .where(UserSubscription.user_id == user_id)
        .order_by(Source.kind, Source.title)
    ).all()

    result: list[dict[str, Any]] = []
    for src, sub in rows:
        labels, creator = infer_source_labels_and_creator(
            kind=src.kind,
            title=src.title,
            url=src.url,
            category_slug=sub.category_slug,
            existing_meta=src.metadata_json,
        )
        result.append(
            {
                "source_id": str(src.id),
                "subscription_id": str(sub.id),
                "kind": src.kind,
                "title": src.title,
                "url": src.url,
                "site_url": src.site_url,
                "sender_email": src.sender_email,
                "list_id": src.list_id,
                "category": sub.category_slug,
                "labels": labels,
                "creator": creator,
                "enabled": sub.enabled,
                "is_public": src.is_public,
                "discovery_method": sub.discovery_method,
                "item_count": src.item_count,
                "last_fetched_at": src.last_fetched_at.isoformat() if src.last_fetched_at else None,
            }
        )
    return result
