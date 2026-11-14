from __future__ import annotations

import email
import imaplib
import logging
import re
from datetime import datetime, timedelta, timezone
from email.header import decode_header
from email.message import Message
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import trafilatura
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from morning_review.connectors.source_registry import register_discovered_newsletter
from morning_review.core.config import settings
from morning_review.db.models import utcnow
from morning_review.pipeline.ingestion import (
    SECTION_PROMO_CONTEXT_RE,
    canonicalize_url,
    ingest_content_item,
    is_promotional_or_self_promo_link,
)

logger = logging.getLogger(__name__)

NEWSLETTER_SENDER_KEYWORDS = (
    "newsletter",
    "substack",
    "beehiiv",
    "ghost",
    "tldr",
    "digest",
    "weekly",
    "briefing",
    "morning",
    "edition",
    "curation",
    "medium",
    "bytebytego",
    "pourlascience",
    "newscientist",
    "canardpc",
    "buttondown",
    "mailchimp",
    "convertkit",
    "revue",
    "infolettre",
    "veille",
    "journal",
)

IGNORED_LINK_DOMAINS = (
    "unsubscribe",
    "list-manage.com",
    "mandrillapp.com",
    "sendgrid.net",
    "mailchimp.com",
    "facebook.com/sharer",
    "twitter.com/intent",
    "x.com/intent",
    "linkedin.com/share",
    "google.com/maps",
    "apple.com/app-store",
    "play.google.com",
    "tldr.tech/tech/manage",
    "tldr.tech/ai/manage",
    "tldr.tech/dev/manage",
    "tldr.tech/devops/manage",
    "tldr.tech/infosec/manage",
    "stay-subscribed",
    "advertise.tldr.tech",
    "jobs.ashbyhq.com/tldr",
)

IGNORED_LINK_TEXT_PATTERNS = re.compile(
    r"^(unsubscribe|se désabonner|se desabonner|manage preferences|view in browser|"
    r"voir dans le navigateur|version en ligne|privacy policy|terms|sponsor|advertise|"
    r"read online|forward|share|tweet|login|sign in|mon compte|abonnement|"
    r".*\(sponsor\).*|.*→.*|.*↗.*|.*click here.*|.*start with .*|.*get free .*|"
    r".*stay subscribed.*|.*remain a subscriber.*|.*apply here.*|.*track your referrals.*)$",
    re.IGNORECASE,
)


def decode_mime_words(raw_val: str | None) -> str:
    if not raw_val:
        return ""
    parts: list[str] = []
    for chunk, charset in decode_header(raw_val):
        if isinstance(chunk, bytes):
            enc = charset or "utf-8"
            try:
                parts.append(chunk.decode(enc, errors="replace"))
            except LookupError:
                parts.append(chunk.decode("utf-8", errors="replace"))
        else:
            parts.append(str(chunk))
    return "".join(parts).strip()


def is_newsletter_message(msg: Message, html_body: str = "", text_body: str = "") -> tuple[bool, str]:
    """Détecte de manière déterministe si un email est une Newsletter (RFC 2369 / 2919 / 8058 + heuristiques)."""
    list_unsub = msg.get("List-Unsubscribe")
    list_id = msg.get("List-Id")
    list_unsub_post = msg.get("List-Unsubscribe-Post")
    precedence = (msg.get("Precedence") or "").lower()
    feedback_id = msg.get("Feedback-ID") or msg.get("X-Feedback-ID")
    x_campaign = msg.get("X-Campaign") or msg.get("X-Campaign-Id") or msg.get("X-Mailgun-Tag")

    from_header = decode_mime_words(msg.get("From", ""))
    _, sender_email = parseaddr(from_header)
    sender_lower = sender_email.lower()
    subject_lower = decode_mime_words(msg.get("Subject", "")).lower()

    # Exclusion des emails purement transactionnels ou administratifs (sécurité, codes OTP, relances)
    excluded_senders = (
        "no-reply@accounts.google.com",
        "noreply@google.com",
        "security@google.com",
    )
    if sender_lower in excluded_senders:
        return False, "sender_excluded"

    transactional_keywords = (
        "verification code",
        "code de vérification",
        "validation en deux",
        "mot de passe d'application",
        "app password",
        "password reset",
        "réinitialisation",
        "facture",
        "invoice",
        "receipt",
        "reçu de paiement",
        "alerte de sécurité",
        "security alert",
        "two-factor",
        "would you like to stay subscribed",
        "confirm your subscription",
    )
    if any(tk in subject_lower for tk in transactional_keywords):
        return False, "transactional_excluded"

    if list_id:
        return True, f"header:List-Id({decode_mime_words(list_id)[:80]})"
    if list_unsub or list_unsub_post:
        return True, "header:List-Unsubscribe"
    if feedback_id or x_campaign:
        return True, "header:Campaign/Feedback-ID"
    if precedence in ("bulk", "list"):
        return True, f"header:Precedence({precedence})"

    if any(k in sender_lower or k in from_header.lower() for k in NEWSLETTER_SENDER_KEYWORDS):
        return True, "sender:newsletter_keyword"
    if any(k in subject_lower for k in ("newsletter", "digest", "edition #", "issue #", "hebdo", "weekly", "briefing")):
        return True, "subject:newsletter_keyword"

    combined_body = (html_body or text_body)[:30000].lower()
    if "unsubscribe" in combined_body or "se désabonner" in combined_body:
        return True, "body:unsubscribe_footer"

    return False, "not_newsletter"


def extract_email_bodies(msg: Message) -> tuple[str, str]:
    html_parts: list[str] = []
    text_parts: list[str] = []

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = str(part.get("Content-Disposition") or "")
            if "attachment" in disp.lower():
                continue
            payload = part.get_payload(decode=True)
            if not payload:
                continue
            charset = part.get_content_charset() or "utf-8"
            try:
                decoded = payload.decode(charset, errors="replace")
            except LookupError:
                decoded = payload.decode("utf-8", errors="replace")
            if ctype == "text/html":
                html_parts.append(decoded)
            elif ctype == "text/plain":
                text_parts.append(decoded)
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            try:
                decoded = payload.decode(charset, errors="replace")
            except LookupError:
                decoded = payload.decode("utf-8", errors="replace")
            if msg.get_content_type() == "text/html":
                html_parts.append(decoded)
            else:
                text_parts.append(decoded)

    return "\n".join(html_parts), "\n".join(text_parts)


SELF_PROMO_EMAIL_SUBJECT_RE = re.compile(
    r"(\bwelcome to\b|\bbienvenue\b|\bthanks for subscribing\b|\bmerci pour votre inscription\b|"
    r"\bupgrade to paid\b|\bspecial offer\b|\boffre spéciale\b|\boffre d'abonnement\b|"
    r"\babonnez-vous\b|\bprofitez de\b|\b\d+%\s*off\b|\bblack friday\b|\bcyber monday\b|"
    r"\bgive a gift\b|\boffrez un abonnement\b|\brefer a friend\b|\bparrainez\b|"
    r"\btake our (?:reader )?survey\b|\bparticipez à notre enquête\b|"
    r"\bdécouvrez nos autres newsletters\b|\bintroducing our new podcast\b)",
    re.IGNORECASE,
)


def is_self_promotional_email(subject: str, sender_name: str = "", text_body: str = "") -> tuple[bool, str]:
    """Détecte si un email entier est un message d'autopromotion de l'éditeur (onboarding, offre d'abonnement, parrainage, enquête)."""
    subj = " ".join((subject or "").split())
    if SELF_PROMO_EMAIL_SUBJECT_RE.search(subj):
        return True, f"self_promo_subject:{subj[:80]}"
    return False, ""


def extract_curated_links_from_html(
    html_body: str,
    newsletter_title: str,
    sender_email: str = "",
) -> list[dict[str, str]]:
    """Extrait les liens éditoriaux unitaires d'une newsletter de curation HTML en excluant
    l'autopromotion du service éditeur et les blocs sponsorisés.
    """
    if not html_body:
        return []
    soup = BeautifulSoup(html_body, "lxml")
    for tag in soup(["script", "style", "head", "footer", "nav"]):
        tag.decompose()

    seen_urls: set[str] = set()
    extracted: list[dict[str, str]] = []

    for a_tag in soup.find_all("a", href=True):
        href = (a_tag.get("href") or "").strip()
        if not href.startswith(("http://", "https://")):
            continue
        if any(dom in href.lower() for dom in IGNORED_LINK_DOMAINS):
            continue

        anchor_text = " ".join(a_tag.get_text(" ", strip=True).split())
        if len(anchor_text) < 18 or IGNORED_LINK_TEXT_PATTERNS.match(anchor_text):
            continue

        norm_url = canonicalize_url(href)
        if any(dom in norm_url.lower() for dom in IGNORED_LINK_DOMAINS):
            continue
        if norm_url in seen_urls:
            continue

        # Cherche le contexte / paragraphe parent comme note du curateur et vérifie les en-têtes de section
        context_text = ""
        parent = a_tag.find_parent(["p", "li", "td", "div", "article", "section"])
        if parent is not None:
            raw_ctx = " ".join(parent.get_text(" ", strip=True).split())
            if SECTION_PROMO_CONTEXT_RE.search(raw_ctx):
                continue
            if len(raw_ctx) > len(anchor_text) + 15:
                context_text = raw_ctx[:600]

        section_container = a_tag.find_parent(["td", "div", "section", "article", "table"])
        if section_container is not None:
            sec_text = " ".join(section_container.get_text(" ", strip=True).split())[:1500]
            if SECTION_PROMO_CONTEXT_RE.search(sec_text):
                continue

        prev_heading = a_tag.find_previous(["h1", "h2", "h3", "h4", "strong", "b"])
        if prev_heading is not None:
            heading_text = " ".join(prev_heading.get_text(" ", strip=True).split())
            if SECTION_PROMO_CONTEXT_RE.search(heading_text):
                continue

        is_promo, _ = is_promotional_or_self_promo_link(
            raw_url=href,
            canonical_url=norm_url,
            title=anchor_text,
            context_text=context_text,
            sender_email=sender_email,
            source_title=newsletter_title,
        )
        if is_promo:
            continue

        seen_urls.add(norm_url)
        extracted.append(
            {
                "title": anchor_text[:280],
                "url": href,
                "canonical_url": norm_url,
                "curator_note": context_text or f"Cité dans {newsletter_title}",
            }
        )
        if len(extracted) >= 15:
            break

    return extracted


def html_to_clean_markdown(html_body: str, fallback_text: str) -> str:
    if html_body:
        extracted = trafilatura.extract(
            html_body,
            output_format="markdown",
            include_links=True,
            include_formatting=True,
        )
        if extracted and len(extracted.strip()) >= 80:
            return extracted.strip()
        soup = BeautifulSoup(html_body, "lxml")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text("\n", strip=True)
        if text:
            return text
    return (fallback_text or "").strip()


def fetch_gmail_newsletters(
    session: Session,
    user_id: UUID,
    *,
    max_messages: int = 100,
    since_days: int = 30,
) -> dict[str, Any]:
    """Se connecte en IMAP SSL à la boîte Gmail fournie dans .env, détecte les Newsletters,
    met à jour automatiquement la liste des flux/ressources et ingère les sujets en BDD + S3.
    """
    account = settings.gmail_account.strip()
    raw_pwd = settings.gmail_pwd.strip()
    if not account or not raw_pwd:
        raise RuntimeError("GMAIL_ACCOUNT ou GMAIL_PWD manquant dans .env")

    # Si l'utilisateur colle un App Password Google au format 'abcd efgh ijkl mnop', on retire les espaces
    password = raw_pwd.replace(" ", "") if len(raw_pwd.replace(" ", "")) == 16 and raw_pwd.replace(" ", "").isalpha() else raw_pwd

    since_date_str = (datetime.now(timezone.utc) - timedelta(days=since_days)).strftime("%d-%b-%Y")

    stats: dict[str, Any] = {
        "scanned_messages": 0,
        "newsletters_detected": 0,
        "promotional_emails_skipped": 0,
        "newsletter_sources_updated": set(),
        "topics_touched": set(),
        "items_ingested": 0,
        "sample_newsletters": [],
    }

    with imaplib.IMAP4_SSL(settings.gmail_imap_host, settings.gmail_imap_port) as imap:
        try:
            imap.login(account, password)
        except imaplib.IMAP4.error as exc:
            raise RuntimeError(
                f"Échec d'authentification IMAP Gmail pour '{account}' ({exc}). "
                "Note : Gmail refuse le mot de passe principal du compte en IMAP et exige un "
                "Mot de passe d'application de 16 lettres généré sur https://myaccount.google.com/apppasswords "
                "dans la variable GMAIL_PWD de .env."
            ) from exc
        imap.select(settings.gmail_curation_label, readonly=True)

        typ, data = imap.search(None, f'(SINCE "{since_date_str}")')
        msg_nums = data[0].split() if (typ == "OK" and data and data[0]) else []
        if not msg_nums:
            typ, data = imap.search(None, "ALL")
            msg_nums = data[0].split() if (typ == "OK" and data and data[0]) else []

        recent_nums = list(reversed(msg_nums[-max_messages:]))

        for num in recent_nums:
            typ, msg_data = imap.fetch(num, "(RFC822)")
            if typ != "OK" or not msg_data or not isinstance(msg_data[0], tuple):
                continue

            raw_bytes = msg_data[0][1]
            msg = email.message_from_bytes(raw_bytes)
            stats["scanned_messages"] += 1

            html_body, text_body = extract_email_bodies(msg)
            is_nl, detection_reason = is_newsletter_message(msg, html_body, text_body)
            if not is_nl:
                continue

            stats["newsletters_detected"] += 1

            from_raw = decode_mime_words(msg.get("From", ""))
            sender_name, sender_email = parseaddr(from_raw)
            sender_name = decode_mime_words(sender_name) or sender_email.split("@")[0]
            subject = decode_mime_words(msg.get("Subject", "Newsletter sans objet"))
            list_id = decode_mime_words(msg.get("List-Id", "")) or None
            message_id = (msg.get("Message-ID") or f"imap-{num.decode()}").strip()

            try:
                pub_dt = parsedate_to_datetime(msg.get("Date")) if msg.get("Date") else utcnow()
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
            except Exception:
                pub_dt = utcnow()

            # 1. Filtre les emails purement autopromotionnels (offres d'abonnement, onboarding, parrainage)
            is_promo_mail, _ = is_self_promotional_email(subject, sender_name, text_body)
            if is_promo_mail:
                stats["promotional_emails_skipped"] += 1
                continue

            # 2. Maintien automatique de la ressource Newsletter dans `sources` + `user_subscriptions`
            source, sub = register_discovered_newsletter(
                session,
                user_id=user_id,
                sender_name=sender_name,
                sender_email=sender_email,
                list_id=list_id,
                subject_hint=subject,
            )
            stats["newsletter_sources_updated"].add(source.title)

            # 3. Extraction des sujets curés (liens éditoriaux multiples filtrés contre l'autopromotion et les sponsors)
            curated_links = extract_curated_links_from_html(
                html_body,
                sender_name,
                sender_email=sender_email,
            )

            # Si la newsletter est un essai unitaire (aucun lien externe curé extrait), on ingère l'édition elle-même
            if not curated_links:
                markdown_edition = html_to_clean_markdown(html_body, text_body)
                edition_url = f"mailto:{sender_email}?message_id={message_id}"
                _, edition_topic, created = ingest_content_item(
                    session,
                    user_id=user_id,
                    source=source,
                    raw_url=edition_url,
                    title=f"[{sender_name}] {subject}",
                    markdown_body=f"# {subject}\n\n*Source : {sender_name} ({sender_email}) — {pub_dt.isoformat()}*\n\n{markdown_edition}",
                    excerpt=markdown_edition[:500],
                    author=sender_name,
                    published_at=pub_dt,
                    external_id=f"edition:{message_id}",
                    curator_note=f"Édition complète de la newsletter {sender_name} ({detection_reason})",
                    visibility="private",
                    category_slug=sub.category_slug,
                )
                stats["topics_touched"].add(str(edition_topic.id))
                if created:
                    stats["items_ingested"] += 1

            for idx, link_info in enumerate(curated_links[:8]):
                link_url = link_info["url"]
                parsed_host = urlparse(link_url).netloc.lower()
                if not parsed_host:
                    continue
                _, link_topic, link_created = ingest_content_item(
                    session,
                    user_id=user_id,
                    source=source,
                    raw_url=link_url,
                    title=link_info["title"],
                    markdown_body=(
                        f"# {link_info['title']}\n\n"
                        f"- **URL** : {link_info['canonical_url']}\n"
                        f"- **Via Newsletter** : {sender_name} (*{subject}*)\n\n"
                        f"## Note de curation\n\n{link_info['curator_note']}\n"
                    ),
                    excerpt=link_info["curator_note"],
                    author=sender_name,
                    published_at=pub_dt,
                    external_id=f"link:{message_id}:{idx}:{link_info['canonical_url'][:120]}",
                    curator_note=link_info["curator_note"],
                    visibility="private",
                    category_slug=sub.category_slug,
                )
                stats["topics_touched"].add(str(link_topic.id))
                if link_created:
                    stats["items_ingested"] += 1

            if len(stats["sample_newsletters"]) < 10:
                stats["sample_newsletters"].append(
                    {
                        "from": f"{sender_name} <{sender_email}>",
                        "subject": subject,
                        "date": pub_dt.isoformat(),
                        "reason": detection_reason,
                        "curated_links_extracted": len(curated_links[:8]),
                        "category": sub.category_slug,
                    }
                )

        session.commit()

    return {
        "scanned_messages": stats["scanned_messages"],
        "newsletters_detected": stats["newsletters_detected"],
        "newsletter_sources_count": len(stats["newsletter_sources_updated"]),
        "newsletter_sources": sorted(stats["newsletter_sources_updated"]),
        "items_ingested": stats["items_ingested"],
        "unique_topics_touched": len(stats["topics_touched"]),
        "sample_newsletters": stats["sample_newsletters"],
    }
