from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from uuid import UUID

import feedparser
import httpx
import trafilatura
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.orm import Session

from morning_review.db.models import Source, UserSubscription, utcnow
from morning_review.pipeline.ingestion import (
    ingest_content_item,
    is_generic_dated_broadcast_placeholder,
    is_promotional_or_self_promo_link,
)

logger = logging.getLogger(__name__)

# Codes officiels des éditions "Read the latest" sur https://www.nytimes.com/newsletters
NYT_NEWSLETTER_EDITIONS: list[tuple[str, str, str]] = [
    ("NN", "NYT The Morning", "actu"),
    ("MBE", "NYT Europe Briefing", "actu"),
    ("DK", "NYT DealBook", "tech"),
    ("INT", "NYT The Interpreter", "actu"),
    ("SC", "NYT Science Times", "science"),
]

# Slugs principaux des éditions "Voir la newsletter" sur https://www.lemonde.fr/newsletters
LEMONDE_NEWSLETTER_SLUGS: dict[str, tuple[str, str]] = {
    "a-la-une": ("Le Monde Newsletter - À la une", "actu"),
    "pixels": ("Le Monde Newsletter - Pixels", "tech"),
    "chaleur-humaine": ("Le Monde Newsletter - Chaleur Humaine", "science"),
    "international": ("Le Monde Newsletter - International", "actu"),
}


def _clean_summary_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "lxml")
    text = " ".join(soup.get_text(" ", strip=True).split())
    text = re.sub(r"Article URL:\s*https?://\S+\s*", "", text)
    text = re.sub(r"Comments URL:\s*https?://\S+\s*", "", text)
    text = re.sub(r"Points:\s*\d+\s*#?\s*Comments:\s*\d+", "", text)
    return " ".join(text.split())


def _parse_feed_entry_datetime(entry: Any) -> datetime:
    """Extrait l'horodatage de publication d'une entrée RSS 2.0 (RFC 2822) ou Atom 1.0 (RFC 3339 / ISO-8601)."""
    for date_attr in ("published", "updated"):
        val = getattr(entry, date_attr, None)
        if not val:
            continue
        raw_str = str(val).strip()
        try:
            dt = parsedate_to_datetime(raw_str)
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
        try:
            iso_str = raw_str[:-1] + "+00:00" if raw_str.endswith("Z") else raw_str
            dt = datetime.fromisoformat(iso_str)
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    for parsed_attr in ("published_parsed", "updated_parsed"):
        st = getattr(entry, parsed_attr, None)
        if st and len(st) >= 6:
            try:
                return datetime(st[0], st[1], st[2], st[3], st[4], st[5], tzinfo=timezone.utc)
            except Exception:
                pass

    return utcnow()


def _ingest_nyt_newsletters_hub(
    session: Session,
    client: httpx.Client,
    user_id: UUID,
    source: Source,
    default_category: str,
) -> tuple[int, set[str]]:
    items_ingested = 0
    topics_touched: set[str] = set()

    for code, nl_label, cat_slug in NYT_NEWSLETTER_EDITIONS:
        sample_url = f"https://static.nytimes.com/email-content/{code}_sample.html"
        try:
            resp = client.get(sample_url)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            raw_title = soup.title.get_text(" ", strip=True) if soup.title else f"{nl_label} Latest Edition"
            title = raw_title if raw_title.startswith(("The ", "DealBook", "Science", "Friday", "Monday", "Tuesday", "Wednesday", "Thursday")) else f"{nl_label}: {raw_title}"

            md_extracted = trafilatura.extract(resp.text, output_format="markdown", include_links=True) or ""
            clean_lines = [
                line.strip(" |")
                for line in md_extracted.splitlines()
                if len(line.strip(" |")) > 35
            ]
            excerpt = " ".join(clean_lines[:4])[:550]

            # Cherche le premier lien d'article NYT éditorial cité dans l'édition
            primary_article_url = sample_url
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "nytimes.com/20" in href and not any(x in href for x in ("unsubscribe", "privacy", "help.")):
                    primary_article_url = href.split("?")[0]
                    break

            md_body = (
                f"# {title}\n\n"
                f"- **Newsletter NYT** : {nl_label} (`{sample_url}`)\n"
                f"- **Article principal** : {primary_article_url}\n\n"
                f"{md_extracted}\n"
            )
            _, topic, created = ingest_content_item(
                session,
                user_id=user_id,
                source=source,
                raw_url=primary_article_url,
                title=title,
                markdown_body=md_body,
                excerpt=excerpt,
                published_at=utcnow(),
                external_id=f"nyt-nl-{code}-{title[:120]}",
                visibility="public",
                category_slug=cat_slug or default_category,
            )
            topics_touched.add(str(topic.id))
            if created:
                items_ingested += 1
        except Exception as exc:
            logger.warning("NYT newsletter sample error (%s): %s", code, exc)

    source.last_fetched_at = utcnow()
    return items_ingested, topics_touched


def _ingest_lemonde_newsletters_hub(
    session: Session,
    client: httpx.Client,
    user_id: UUID,
    source: Source,
    default_category: str,
    max_articles_per_edition: int = 4,
) -> tuple[int, set[str]]:
    items_ingested = 0
    topics_touched: set[str] = set()

    resp = client.get(source.url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    preview_urls: list[tuple[str, str, str]] = []
    seen_urls: set[str] = set()
    for btn in soup.find_all(attrs={"data-preview-url": True}):
        p_url = btn.get("data-preview-url", "").strip()
        if not p_url or p_url in seen_urls:
            continue
        for slug, (nl_label, cat_slug) in LEMONDE_NEWSLETTER_SLUGS.items():
            if f"/newsletters/{slug}/" in p_url:
                seen_urls.add(p_url)
                preview_urls.append((p_url, nl_label, cat_slug))
                break

    for p_url, nl_label, cat_slug in preview_urls:
        try:
            ed_resp = client.get(p_url)
            if ed_resp.status_code != 200:
                continue
            ed_soup = BeautifulSoup(ed_resp.text, "lxml")
            count_in_ed = 0
            seen_in_ed: set[str] = set()
            for a in ed_soup.find_all("a", href=True):
                href = a["href"].split("?")[0]
                if "lemonde.fr/" not in href or "/article/" not in href:
                    continue
                if href in seen_in_ed:
                    continue
                link_title = " ".join(a.get_text(" ", strip=True).split())
                if len(link_title) < 28 or link_title.lower().startswith(("lire", "voir", "abonn", "découvrir")):
                    continue

                is_promo, _ = is_promotional_or_self_promo_link(
                    raw_url=a["href"],
                    canonical_url=href,
                    title=link_title,
                    source_title=nl_label,
                )
                if is_promo:
                    continue

                seen_in_ed.add(href)
                parent_text = ""
                parent = a.find_parent(["td", "div", "p", "li"])
                if parent:
                    parent_text = " ".join(parent.get_text(" ", strip=True).split())
                    if parent_text.startswith(link_title):
                        parent_text = parent_text[len(link_title) :].strip(" .-–:")

                excerpt = parent_text[:450] if len(parent_text) >= 25 else f"[{nl_label}] {link_title}"
                md_body = (
                    f"# {link_title}\n\n"
                    f"- **Édition Newsletter** : {nl_label} ({p_url})\n"
                    f"- **Lien** : {href}\n\n"
                    f"{excerpt}\n"
                )
                _, topic, created = ingest_content_item(
                    session,
                    user_id=user_id,
                    source=source,
                    raw_url=href,
                    title=link_title,
                    markdown_body=md_body,
                    excerpt=excerpt,
                    published_at=utcnow(),
                    external_id=f"lm-nl::{href}"[:500],
                    visibility="public",
                    category_slug=cat_slug or default_category,
                )
                topics_touched.add(str(topic.id))
                if created:
                    items_ingested += 1
                count_in_ed += 1
                if count_in_ed >= max_articles_per_edition:
                    break
        except Exception as exc:
            logger.warning("Le Monde newsletter edition error (%s): %s", p_url, exc)

    source.last_fetched_at = utcnow()
    return items_ingested, topics_touched


def _ingest_microsoft_status_hub(
    session: Session,
    client: httpx.Client,
    user_id: UUID,
    source: Source,
    default_category: str,
) -> tuple[int, set[str]]:
    """Interroge l'API JSON officielle de https://status.cloud.microsoft/m365
    (`/api/posts/m365Consumer` et `/api/posts/mac`) pour ingérer les incidents majeurs
    et leur statut de résolution ("Service restored").
    """
    items_ingested = 0
    topics_touched: set[str] = set()

    try:
        resp = client.get("https://status.cloud.microsoft/api/posts/m365Consumer", timeout=10.0)
        if resp.status_code == 200:
            entries = resp.json()
            if isinstance(entries, list):
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    raw_title = (entry.get("Title") or "").strip()
                    raw_msg = (entry.get("Message") or "").strip()
                    status_str = (entry.get("Status") or "Operational").strip()
                    service_name = (entry.get("ServiceDisplayName") or "Microsoft 365").strip()
                    workload = (entry.get("ServiceWorkloadName") or "M365Consumer").strip()
                    last_updated = (entry.get("LastUpdatedTime") or "").strip()

                    # On ignore les workloads purement nominaux sans message d'incident ni de rétablissement
                    if not raw_title and not raw_msg:
                        continue

                    pub_dt = utcnow()
                    if last_updated and not last_updated.startswith("0001-"):
                        try:
                            iso_str = last_updated[:-1] + "+00:00" if last_updated.endswith("Z") else last_updated
                            dt = datetime.fromisoformat(iso_str)
                            pub_dt = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
                        except Exception:
                            pass

                    clean_msg = " ".join(raw_msg.split())
                    if status_str.lower() in ("service restored", "restored", "resolved", "available"):
                        title = (
                            f"Panne Microsoft 365 / Outlook résolue ({status_str}) — "
                            f"{service_name} : {raw_title or 'Service intégralement rétabli'}"
                        )
                    else:
                        title = f"Incident {service_name} ({status_str}) : {raw_title or clean_msg[:100]}"

                    excerpt = f"[{service_name} — {status_str}] {clean_msg}"[:580]
                    status_slug = re.sub(r"[^a-z0-9]+", "-", status_str.lower()).strip("-")
                    item_url = f"https://status.cloud.microsoft/m365?workload={workload}&status={status_slug}"
                    ext_id = f"ms-status::{workload}::{status_slug}::{last_updated[:16]}"
                    md_body = (
                        f"# {title}\n\n"
                        f"- **Source officielle** : Microsoft Service Health (`https://status.cloud.microsoft/m365`)\n"
                        f"- **Service** : {service_name} (`{workload}`)\n"
                        f"- **Statut** : **{status_str}**\n"
                        f"- **Dernière mise à jour** : {last_updated}\n\n"
                        f"{raw_msg}\n"
                    )
                    _, topic, created = ingest_content_item(
                        session,
                        user_id=user_id,
                        source=source,
                        raw_url=item_url,
                        title=title,
                        markdown_body=md_body,
                        excerpt=excerpt,
                        published_at=pub_dt,
                        external_id=ext_id[:500],
                        visibility="public",
                        category_slug=default_category or "tech",
                    )
                    topics_touched.add(str(topic.id))
                    if created:
                        items_ingested += 1
    except Exception as exc:
        logger.warning("Microsoft status hub fetch error: %s", exc)

    source.last_fetched_at = utcnow()
    return items_ingested, topics_touched


def fetch_rss_sources(
    session: Session,
    user_id: UUID,
    *,
    max_entries_per_feed: int = 10,
    include_disabled_creators: bool = False,
) -> dict[str, Any]:
    """Relève les flux RSS, Podcasts, chaînes YouTube et pages d'éditions de newsletters publiques (NYT, Le Monde) actifs."""
    stmt = (
        select(Source, UserSubscription)
        .join(UserSubscription, UserSubscription.source_id == Source.id)
        .where(
            UserSubscription.user_id == user_id,
            Source.kind.in_(("rss", "web", "podcast", "youtube")),
        )
    )
    if not include_disabled_creators:
        stmt = stmt.where(UserSubscription.enabled.is_(True))
    rows = session.execute(stmt).all()

    feeds_polled = 0
    items_ingested = 0
    topics_touched: set[str] = set()
    errors: list[str] = []

    with httpx.Client(
        timeout=12.0,
        follow_redirects=True,
        headers={"User-Agent": "MorningReview/1.0 (RSS Reader; Feed Fetcher)"},
    ) as client:
        for source, sub in rows:
            # Cas 1 : Annuaire des dernières éditions de newsletters du New York Times
            if "nytimes.com/newsletters" in source.url:
                try:
                    feeds_polled += 1
                    ing, touched = _ingest_nyt_newsletters_hub(
                        session,
                        client,
                        user_id,
                        source,
                        sub.category_slug,
                    )
                    items_ingested += ing
                    topics_touched |= touched
                except Exception as exc:
                    errors.append(f"{source.title}: {exc}")
                continue

            # Cas 2 : Annuaire des dernières éditions de newsletters du Monde
            if "lemonde.fr/newsletters" in source.url:
                try:
                    feeds_polled += 1
                    ing, touched = _ingest_lemonde_newsletters_hub(
                        session,
                        client,
                        user_id,
                        source,
                        sub.category_slug,
                    )
                    items_ingested += ing
                    topics_touched |= touched
                except Exception as exc:
                    errors.append(f"{source.title}: {exc}")
                continue

            # Cas 2b : Page officielle de statut Microsoft 365 (https://status.cloud.microsoft/m365)
            if "status.cloud.microsoft" in source.url:
                try:
                    feeds_polled += 1
                    ing, touched = _ingest_microsoft_status_hub(
                        session,
                        client,
                        user_id,
                        source,
                        sub.category_slug,
                    )
                    items_ingested += ing
                    topics_touched |= touched
                except Exception as exc:
                    errors.append(f"{source.title}: {exc}")
                continue

            # Cas 3 : Flux RSS / Atom standard, Podcast RSS ou Flux Atom YouTube
            feed_url = source.url
            if "thenewstack.io/rss-feeds" in feed_url:
                feed_url = "https://thenewstack.io/blog/feed/"

            req_headers: dict[str, str] = {}
            if source.etag:
                req_headers["If-None-Match"] = source.etag
            if source.last_modified:
                req_headers["If-Modified-Since"] = source.last_modified

            try:
                resp = client.get(feed_url, headers=req_headers)
                feeds_polled += 1
                if resp.status_code == 304:
                    source.last_fetched_at = utcnow()
                    continue
                resp.raise_for_status()
            except Exception as exc:
                logger.warning("RSS fetch error on %s: %s", feed_url, exc)
                errors.append(f"{source.title}: {exc}")
                continue

            source.etag = resp.headers.get("ETag") or source.etag
            source.last_modified = resp.headers.get("Last-Modified") or source.last_modified
            parsed = feedparser.parse(resp.text)

            meta_labels = (source.metadata_json or {}).get("labels") or []
            is_creator_media = source.kind in ("podcast", "youtube") or "podcast" in meta_labels or "youtube" in meta_labels
            feed_limit = min(max_entries_per_feed, 3) if is_creator_media else max_entries_per_feed
            now_utc = utcnow()

            for idx, entry in enumerate((parsed.entries or [])[:feed_limit]):
                link = getattr(entry, "link", "") or ""
                if not link:
                    enclosures = getattr(entry, "enclosures", None) or []
                    for enc in enclosures:
                        href = enc.get("href") if isinstance(enc, dict) else getattr(enc, "href", "")
                        if href and str(href).startswith("http"):
                            link = str(href)
                            break
                if not link:
                    link = source.site_url or source.url

                title = getattr(entry, "title", "") or "Sans titre"
                if "news.google.com/rss/" in feed_url:
                    title = re.sub(
                        r"\s+-\s+(?:L'Usine Digitale|L'Usine Nouvelle|Usine Digitale|Usine Nouvelle)\s*$",
                        "",
                        title,
                        flags=re.IGNORECASE,
                    ).strip()
                raw_summary = (
                    getattr(entry, "summary", "")
                    or getattr(entry, "description", "")
                    or getattr(entry, "media_description", "")
                    or ""
                )
                full_content_html = ""
                if getattr(entry, "content", None):
                    first_c = entry.content[0] if isinstance(entry.content, list) and entry.content else None
                    if isinstance(first_c, dict):
                        full_content_html = first_c.get("value") or ""
                    elif hasattr(first_c, "value"):
                        full_content_html = getattr(first_c, "value") or ""
                if not raw_summary and full_content_html:
                    raw_summary = full_content_html
                clean_excerpt = _clean_summary_html(raw_summary)
                if "news.google.com/rss/" in feed_url:
                    clean_excerpt = re.sub(
                        r"\s+(?:L'Usine Digitale|L'Usine Nouvelle|Usine Digitale|Usine Nouvelle)\s*$",
                        "",
                        clean_excerpt,
                        flags=re.IGNORECASE,
                    ).strip()
                entry_id = getattr(entry, "id", "") or link or title

                # Filtre les grilles horaires radio génériques sans description éditoriale (ex: "JOURNAL DE 09H00 du samedi...")
                if is_generic_dated_broadcast_placeholder(title, clean_excerpt):
                    continue

                # Filtre les articles taggés comme sponsorisés dans les catégories RSS (ex: The New Stack 'sponsor-*', 'Sponsored')
                entry_tags = [
                    str(t.get("term", "") or t.get("label", "") or "").lower()
                    for t in (getattr(entry, "tags", None) or [])
                    if isinstance(t, dict)
                ]
                if any(
                    marker in tag_val
                    for tag_val in entry_tags
                    for marker in ("sponsor", "promoted", "advertorial", "publicité", "partenaire", "bon plan", "bons plans")
                ):
                    continue

                if not is_creator_media:
                    is_promo, _ = is_promotional_or_self_promo_link(
                        raw_url=link,
                        canonical_url=link,
                        title=title,
                        context_text=clean_excerpt,
                        source_title=source.title,
                    )
                    if is_promo:
                        continue

                pub_dt = _parse_feed_entry_datetime(entry)
                if is_creator_media and idx > 0 and (now_utc - pub_dt).days > 45:
                    continue
                if not is_creator_media and idx > 0 and (now_utc - pub_dt).days > 90:
                    continue

                # Si le flux RSS fournit le corps complet dans <content:encoded> (ex: WordPress / Cafzone),
                # on l'extrait directement en Markdown pour disposer de l'article intégral dans S3
                extra_body_md = ""
                if full_content_html and len(full_content_html) > len(raw_summary) + 200:
                    extracted_full = (
                        trafilatura.extract(f"<html><body>{full_content_html}</body></html>", output_format="markdown")
                        or _clean_summary_html(full_content_html)
                    )
                    if len(extracted_full.strip()) > len(clean_excerpt):
                        extra_body_md = extracted_full.strip()

                # Si un article provient de Pour la Science et que le jeton abonné est actif,
                # récupère le texte abonné intégral dès l'ingestion lorsque l'article n'est pas encore enrichi
                if "pourlascience.fr/" in link:
                    from morning_review.connectors.mcp_enricher import (
                        _fetch_via_agent_auth_mcp,
                        _get_runtime_token_path,
                    )
                    from morning_review.db.models import CanonicalItem
                    from morning_review.pipeline.ingestion import canonicalize_url

                    pls_token = _get_runtime_token_path("pourlascience")
                    if pls_token.exists():
                        canon_link = canonicalize_url(link)
                        existing_ci = session.scalar(
                            select(CanonicalItem).where(CanonicalItem.canonical_url == canon_link)
                        )
                        already_enriched = bool(
                            existing_ci and (existing_ci.s3_key or "").startswith("enriched/")
                        )
                        if not already_enriched:
                            try:
                                pls_data = _fetch_via_agent_auth_mcp(
                                    "tools.pourlascience.client:PourLaScienceClient",
                                    link,
                                )
                                pls_md = (pls_data.get("markdown_body") or "").strip()
                                if len(pls_md) > 400:
                                    extra_body_md = pls_md
                                    paras = [
                                        " ".join(p.split())
                                        for p in pls_md.split("\n\n")
                                        if len(p.strip()) >= 50 and not p.strip().startswith("#") and not p.strip().startswith("[")
                                    ]
                                    if paras:
                                        clean_excerpt = " ".join(paras[:2])[:500]
                            except Exception as exc:
                                logger.debug("Pour la Science inline subscriber fetch skipped for %s: %s", link, exc)

                # Si l'article provient d'une source de fond (Quanta Magazine, Pour la Science, New Scientist, Ars Technica...)
                # ou que le flux RSS ne fournit qu'un court chapô (< 220 car.), extrait le texte intégral via Trafilatura
                is_longform_domain = any(
                    d in link.lower()
                    for d in (
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
                        "lwn.net",
                        "danluu.com",
                        "simonwillison.net",
                    )
                )
                if not extra_body_md and (is_longform_domain or len(clean_excerpt) < 220) and link.startswith(("http://", "https://")):
                    if "youtube.com/" in link or "youtu.be/" in link:
                        creator_lbl = str((source.metadata_json or {}).get("creator") or source.title.replace("(YouTube)", "").strip())
                        clean_excerpt = f"Vidéo publiée par {creator_lbl} : « {title} »."
                    elif "radiofrance.fr/" not in link:
                        try:
                            art_resp = client.get(link, timeout=7.0)
                            if art_resp.status_code == 200 and "text/html" in art_resp.headers.get("content-type", "").lower():
                                extracted_md = (
                                    trafilatura.extract(
                                        art_resp.text,
                                        output_format="markdown",
                                        include_links=False,
                                        favor_precision=True,
                                    )
                                    or ""
                                )
                                paras = [
                                    " ".join(p.split())
                                    for p in extracted_md.split("\n\n")
                                    if len(p.strip()) >= 45 and not p.strip().startswith("#")
                                ]
                                if paras:
                                    clean_excerpt = " ".join(paras[:3])[:560]
                                    extra_body_md = "\n\n".join(paras[:14])
                        except Exception:
                            pass

                body_content = extra_body_md or clean_excerpt
                md_body = (
                    f"# {title}\n\n"
                    f"- **Source RSS** : {source.title}\n"
                    f"- **Lien** : {link}\n\n"
                    f"{body_content}\n"
                )
                _, topic, created = ingest_content_item(
                    session,
                    user_id=user_id,
                    source=source,
                    raw_url=link,
                    title=title,
                    markdown_body=md_body,
                    excerpt=clean_excerpt[:500],
                    author=getattr(entry, "author", None),
                    published_at=pub_dt,
                    external_id=str(entry_id)[:500],
                    visibility="public" if source.is_public else "private",
                    category_slug=sub.category_slug,
                )
                topics_touched.add(str(topic.id))
                if created:
                    items_ingested += 1

            session.commit()

        session.commit()

    return {
        "feeds_polled": feeds_polled,
        "items_ingested": items_ingested,
        "unique_topics_touched": len(topics_touched),
        "errors": errors,
    }
