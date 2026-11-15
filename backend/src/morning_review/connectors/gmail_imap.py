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
from urllib.parse import urljoin, urlparse
from uuid import UUID

import httpx
import trafilatura
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.orm import Session

from morning_review.connectors.source_registry import register_discovered_newsletter
from morning_review.core.config import settings
from morning_review.db.models import NewsletterConfirmation, utcnow
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


def unwrap_forwarded_email_metadata(
    msg: Message,
    html_body: str = "",
    text_body: str = "",
) -> dict[str, Any]:
    """Extrait l'expéditeur, le sujet et la date d'origine réels d'un email, y compris
    lorsqu'il a été transféré manuellement ('Fwd:' / '---------- Forwarded message ---------')
    ou automatiquement via une règle de transfert Gmail.
    """
    outer_from = " ".join(decode_mime_words(msg.get("From", "")).split())
    outer_name, outer_email = parseaddr(outer_from)
    outer_name = " ".join(decode_mime_words(outer_name).split()) or (outer_email.split("@")[0] if outer_email else "")
    outer_subject = " ".join(decode_mime_words(msg.get("Subject", "Newsletter sans objet")).split())

    clean_subject = re.sub(r"^(?:(?:fwd|fw|tr)\s*:\s*)+", "", outer_subject, flags=re.IGNORECASE).strip()
    is_forwarded = bool(
        re.match(r"^(?:fwd|fw|tr)\s*:", outer_subject, flags=re.IGNORECASE)
        or "forwarded message" in (text_body[:2500] or html_body[:6000]).lower()
        or "message transféré" in (text_body[:2500] or html_body[:6000]).lower()
    )

    eff_name = outer_name
    eff_email = outer_email.lower().strip()
    eff_subject = clean_subject or outer_subject

    if is_forwarded:
        head_txt = text_body[:3000] if text_body else ""
        if not head_txt and html_body:
            soup_head = BeautifulSoup(html_body[:12000], "lxml")
            head_txt = soup_head.get_text("\n", strip=True)[:3000]

        m_from = re.search(
            r"(?:^|\n)\s*(?:De\s*:|From\s*:)\s*([^\r\n]+)",
            head_txt,
            flags=re.IGNORECASE,
        )
        if m_from:
            raw_inner_from = m_from.group(1).strip()
            in_name, in_email = parseaddr(raw_inner_from)
            if in_email and "@" in in_email:
                eff_email = in_email.lower().strip()
                eff_name = decode_mime_words(in_name).strip() or eff_email.split("@")[0]

        m_subj = re.search(
            r"(?:^|\n)\s*(?:Subject\s*:|Objet\s*:)\s*([^\r\n]+(?:\r?\n(?!(?:To|À|Date|De|From|Cc)\s*:)[^\r\n]+)*)",
            head_txt,
            flags=re.IGNORECASE,
        )
        if m_subj:
            inner_subj = " ".join(m_subj.group(1).split()).strip()
            if len(inner_subj) > len(eff_subject):
                eff_subject = inner_subj

    # Fallback spécial Medium Daily Digest si transféré sans en-tête texte standard
    combined_sample = f"{html_body[:15000]} {text_body[:6000]}".lower()
    if "digest.reader" in combined_sample and "medium.com" in combined_sample:
        if not eff_email or eff_email == settings.gmail_account.strip().lower():
            eff_email = "noreply@medium.com"
            eff_name = "Medium Daily Digest"

    # Date de publication : pour Medium Daily Digest, le paramètre `source=email-<id>-<epoch_ms>-digest.reader`
    # contient le timestamp exact d'émission du digest par Medium.
    pub_dt: datetime | None = None
    m_medium_ts = re.search(
        r"source=email-[a-f0-9]+-(\d{13})-digest\.reader",
        html_body or text_body,
    )
    if m_medium_ts:
        try:
            pub_dt = datetime.fromtimestamp(int(m_medium_ts.group(1)) / 1000.0, tz=timezone.utc)
        except Exception:
            pub_dt = None

    if pub_dt is None:
        try:
            pub_dt = parsedate_to_datetime(msg.get("Date")) if msg.get("Date") else utcnow()
            if pub_dt.tzinfo is None:
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)
        except Exception:
            pub_dt = utcnow()

    return {
        "sender_name": eff_name or "Newsletter",
        "sender_email": eff_email,
        "subject": eff_subject,
        "published_at": pub_dt,
        "is_forwarded": is_forwarded,
    }


def is_newsletter_message(msg: Message, html_body: str = "", text_body: str = "") -> tuple[bool, str]:
    """Détecte de manière déterministe si un email est une Newsletter (RFC 2369 / 2919 / 8058 + heuristiques
    + support des newsletters transférées manuellement ou par filtre Gmail).
    """
    list_unsub = msg.get("List-Unsubscribe")
    list_id = msg.get("List-Id")
    list_unsub_post = msg.get("List-Unsubscribe-Post")
    precedence = (msg.get("Precedence") or "").lower()
    feedback_id = msg.get("Feedback-ID") or msg.get("X-Feedback-ID")
    x_campaign = msg.get("X-Campaign") or msg.get("X-Campaign-Id") or msg.get("X-Mailgun-Tag")

    unwrapped = unwrap_forwarded_email_metadata(msg, html_body, text_body)
    sender_name = unwrapped["sender_name"]
    sender_lower = unwrapped["sender_email"].lower()
    subject_lower = unwrapped["subject"].lower()

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

    if any(k in sender_lower or k in sender_name.lower() for k in NEWSLETTER_SENDER_KEYWORDS):
        prefix = "forwarded_sender" if unwrapped["is_forwarded"] else "sender"
        return True, f"{prefix}:{sender_name} <{sender_lower}>"
    if any(k in subject_lower for k in ("newsletter", "digest", "edition #", "issue #", "hebdo", "weekly", "briefing")):
        return True, "subject:newsletter_keyword"

    # Inspecte le début ET la fin du message (les newsletters longues comme Medium Daily Digest font ~80 Ko)
    raw_content = html_body or text_body
    combined_body = (raw_content[:30000] + "\n" + raw_content[-20000:]).lower()
    if (
        "unsubscribe" in combined_body
        or "se désabonner" in combined_body
        or "digest.reader" in combined_body
    ):
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


SECURITY_OR_AUTH_REJECT_RE = re.compile(
    r"(\bsign[\s-]?in\b|\blog[\s-]?in\b|\bconnexion\b|\bmagic link\b|\bpassword\b|\bmot de passe\b|"
    r"\breset\b|\bréinitialisation\b|\btwo[\s-]?factor\b|\b2fa\b|\bverification code\b|"
    r"\bcode de vérification\b|\bsecurity alert\b|\balerte de sécurité\b|\bchange email\b|"
    r"\bdelete account\b|\bsupprimer\b|\binvoice\b|\bfacture\b|\breceipt\b|\bpaiement\b|"
    r"\boauth\b|\bauthorize\b|\bstay subscribed\b)",
    re.IGNORECASE,
)

OPTIN_SUBJECT_MATCH_RE = re.compile(
    r"(confirm(?:ez|er)?\s+(?:your\s+)?(?:subscription|sign[\s-]?up|email|newsletter|inscription|abonnement)|"
    r"valide[rz]\s+(?:votre|mon)\s+(?:inscription|abonnement|adresse)|"
    r"activate\s+your\s+(?:subscription|sign[\s-]?up)|"
    r"complete\s+your\s+(?:subscription|sign[\s-]?up)|"
    r"verify\s+your\s+email\s+(?:address\s+)?to\s+(?:subscribe|receive|sign\s+up)|"
    r"please\s+confirm\s+(?:subscription|sign[\s-]?up)|confirmer\s+l['’]inscription)",
    re.IGNORECASE,
)

OPTIN_BODY_MATCH_RE = re.compile(
    r"(confirm\s+(?:your\s+)?(?:subscription|sign[\s-]?up)|confirmez?\s+votre\s+(?:inscription|abonnement)|"
    r"complete\s+the\s+sign[\s-]?up\s+process|"
    r"yes,\s*subscribe\s+me|oui,\s*je\s+m['’]inscris|valider\s+mon\s+inscription|"
    r"click\s+(?:the\s+(?:button|link)\s+)?below\s+to\s+confirm\s+your\s+subscription)",
    re.IGNORECASE,
)

CAPTCHA_OR_ANTIBOT_RE = re.compile(
    r"(g-recaptcha|hcaptcha|cf-turnstile|challenges\.cloudflare\.com|challenge-platform|"
    r"_cf_chl_opt|verify\s+you\s+are\s+human|vérifiez\s+que\s+vous\s+êtes\s+humain|"
    r"confirmez\s+que\s+vous\s+êtes\s+humain|are\s+you\s+a\s+robot)",
    re.IGNORECASE,
)

CONFIRMATION_SUCCESS_RE = re.compile(
    r"(subscription\s+confirmed|you(?:'re|\s+are)\s+(?:now\s+)?subscribed|"
    r"thanks?\s+for\s+(?:confirming|subscribing)|thank\s+you\s+for\s+(?:confirming|subscribing)|"
    r"email\s+(?:address\s+)?confirmed|inscription\s+(?:confirmée|validée)|"
    r"votre\s+(?:inscription|abonnement)\s+(?:est|a\s+été)\s+(?:confirmée?|validée?)|"
    r"merci\s+(?:d['’]avoir\s+confirmé|pour\s+votre\s+inscription)|abonnement\s+confirmé)",
    re.IGNORECASE,
)


def is_newsletter_optin_confirmation_email(
    msg: Message,
    html_body: str = "",
    text_body: str = "",
) -> bool:
    """Identifie strictement un email de confirmation d'inscription newsletter (double opt-in),
    en rejetant tout email d'authentification, sécurité, compte ou paiement.
    """
    unwrapped = unwrap_forwarded_email_metadata(msg, html_body, text_body)
    sender_lower = unwrapped["sender_email"].lower()
    subject = unwrapped["subject"]

    blocked_sender_domains = (
        "accounts.google.com",
        "google.com",
        "github.com",
        "apple.com",
        "microsoft.com",
        "amazon.com",
        "paypal.com",
        "stripe.com",
    )
    if any(sender_lower.endswith("@" + d) or sender_lower.endswith("." + d) for d in blocked_sender_domains):
        return False

    if SECURITY_OR_AUTH_REJECT_RE.search(subject):
        return False

    if OPTIN_SUBJECT_MATCH_RE.search(subject):
        return True

    # Sinon, vérifie le corps si l'objet évoque une confirmation / vérification d'inscription
    if re.search(r"\b(confirm|confirmer|confirmation|verify|vérifier|valider|welcome|inscription|subscription)\b", subject, re.IGNORECASE):
        sample_text = text_body[:4000]
        if not sample_text and html_body:
            sample_text = BeautifulSoup(html_body[:15000], "lxml").get_text(" ", strip=True)[:4000]
        if SECURITY_OR_AUTH_REJECT_RE.search(sample_text):
            return False
        if OPTIN_BODY_MATCH_RE.search(sample_text):
            return True

    return False


def extract_newsletter_confirmation_url(html_body: str, text_body: str = "") -> str | None:
    """Extrait l'URL du bouton/lien de validation d'inscription (en excluant strictement tout lien de désabonnement)."""
    bad_tokens = (
        "unsubscribe",
        "desabonner",
        "désabonner",
        "optout",
        "opt-out",
        "action=signin",
        "privacy",
        "terms",
        "preferences",
        "manage",
        "view-in-browser",
        "facebook.com",
        "twitter.com",
        "x.com",
        "linkedin.com",
        "instagram.com",
        "mailto:",
    )
    best_url: str | None = None
    best_score = 0

    if html_body:
        soup = BeautifulSoup(html_body, "lxml")
        for a_tag in soup.find_all("a", href=True):
            href = (a_tag.get("href") or "").strip()
            if not href.startswith(("http://", "https://")):
                continue
            anchor_txt = " ".join(a_tag.get_text(" ", strip=True).split())
            combined_low = f"{href} {anchor_txt}".lower()
            if any(bt in combined_low for bt in bad_tokens):
                continue
            parsed = urlparse(href)
            if (not parsed.path or parsed.path == "/") and not parsed.query:
                continue

            score = 0
            if re.search(
                r"(confirm|confirmer|valider|subscribe|sign[\s-]?up|opt[-_]?in|verify|vérifier|activate|activer|oui)",
                anchor_txt,
                re.IGNORECASE,
            ):
                score += 10
            if re.search(
                r"(confirm|opt[-_]?in|subscribe/confirm|action=signup|action=subscribe|verify|activate|validation|double-opt)",
                href,
                re.IGNORECASE,
            ):
                score += 8
            style_attr = f"{a_tag.get('style') or ''} {' '.join(a_tag.get('class') or [])}".lower()
            if any(k in style_attr for k in ("button", "btn", "background")):
                score += 4

            if score > best_score:
                best_score = score
                best_url = href

    if best_url and best_score >= 4:
        return best_url

    if text_body:
        for m in re.finditer(r"https?://[^\s<>()\"']+", text_body):
            u = m.group(0).rstrip(".,;)>]")
            u_low = u.lower()
            if any(bt in u_low for bt in bad_tokens):
                continue
            if re.search(r"(confirm|opt[-_]?in|action=signup|action=subscribe|verify|activate|valider)", u_low):
                return u

    return None


CONFIRMATION_REDIRECT_QUERY_RE = re.compile(
    r"((?:action=(?:signup|subscribe)[^#]*success=true)|"
    r"(?:success=true[^#]*action=(?:signup|subscribe))|"
    r"(?:[?&](?:subscribed|confirmed|optin_confirmed)=(?:1|true|yes)\b))",
    re.IGNORECASE,
)


def validate_newsletter_confirmation_url(
    url: str,
    *,
    timeout: float = 12.0,
    http_client: httpx.Client | None = None,
) -> dict[str, str]:
    """Valide prudemment un lien double opt-in (GET + éventuel formulaire POST simple en 2 étapes).
    Ne tente JAMAIS de contourner un CAPTCHA ou défi anti-bot : renvoie alors `status='manual_required'`.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) MorningReview/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    own_client = http_client is None
    client = http_client or httpx.Client(follow_redirects=True, timeout=timeout, headers=headers)
    try:
        resp = client.get(url)
        final_url = str(resp.url)
        body_html = resp.text or ""

        if resp.status_code in (401, 403, 429, 503) or CAPTCHA_OR_ANTIBOT_RE.search(body_html):
            return {
                "status": "manual_required",
                "final_url": final_url,
                "detail": f"Défi anti-bot ou CAPTCHA détecté (HTTP {resp.status_code}) — validation manuelle requise",
            }

        soup = BeautifulSoup(body_html, "lxml")
        visible_text = soup.get_text(" ", strip=True)
        if resp.status_code < 400 and (
            CONFIRMATION_SUCCESS_RE.search(visible_text) or CONFIRMATION_REDIRECT_QUERY_RE.search(final_url)
        ):
            return {
                "status": "confirmed",
                "final_url": final_url,
                "detail": "Confirmé automatiquement par requête HTTP GET",
            }

        # Support du pattern en 2 étapes (RFC 9110 : page intermédiaire avec formulaire POST sans CAPTCHA ni champs texte)
        for form in soup.find_all("form"):
            form_txt = form.get_text(" ", strip=True)
            action_raw = (form.get("action") or "").strip()
            action_url = urljoin(final_url, action_raw) if action_raw else final_url
            if any(bad in f"{action_url} {form_txt}".lower() for bad in ("unsubscribe", "désabonner", "desabonner", "cancel")):
                continue
            if not re.search(
                r"(confirm|subscribe|sign[\s-]?up|valider|confirmer|oui|yes|opt[-_]?in|continuer)",
                f"{action_url} {form_txt}",
                re.IGNORECASE,
            ):
                continue

            # Vérifie qu'aucune saisie utilisateur (texte/mot de passe/captcha) n'est exigée
            requires_user_typing = False
            form_data: dict[str, str] = {}
            for inp in form.find_all("input"):
                inp_type = (inp.get("type") or "text").lower()
                inp_name = inp.get("name")
                if inp_type in ("password", "file"):
                    requires_user_typing = True
                    break
                if inp_type in ("text", "email") and inp.has_attr("required") and not inp.get("value"):
                    requires_user_typing = True
                    break
                if inp_name and inp_type in ("hidden", "submit"):
                    form_data[inp_name] = inp.get("value") or ""

            if requires_user_typing:
                continue

            method = (form.get("method") or "post").upper()
            post_resp = (
                client.post(action_url, data=form_data)
                if method == "POST"
                else client.get(action_url, params=form_data)
            )
            post_html = post_resp.text or ""
            if post_resp.status_code in (401, 403, 429, 503) or CAPTCHA_OR_ANTIBOT_RE.search(post_html):
                return {
                    "status": "manual_required",
                    "final_url": str(post_resp.url),
                    "detail": "CAPTCHA demandé à la soumission du formulaire — validation manuelle requise",
                }
            post_text = BeautifulSoup(post_html, "lxml").get_text(" ", strip=True)
            if post_resp.status_code < 400 and (
                CONFIRMATION_SUCCESS_RE.search(post_text) or post_resp.status_code in (200, 201, 204)
            ):
                return {
                    "status": "confirmed",
                    "final_url": str(post_resp.url),
                    "detail": f"Confirmé automatiquement via formulaire {method} en 2 étapes",
                }

        return {
            "status": "manual_required",
            "final_url": final_url,
            "detail": "Page interactive (JavaScript/session) — bouton 1-clic affiché dans Morning Review",
        }
    except Exception as exc:
        return {
            "status": "manual_required",
            "final_url": url,
            "detail": f"Validation HTTP différée ({exc}) — lien direct conservé",
        }
    finally:
        if own_client:
            client.close()


def list_newsletter_confirmations(
    session: Session,
    user_id: UUID,
    *,
    include_dismissed: bool = False,
    limit: int = 20,
) -> list[dict[str, Any]]:
    stmt = select(NewsletterConfirmation).where(NewsletterConfirmation.user_id == user_id)
    if not include_dismissed:
        stmt = stmt.where(NewsletterConfirmation.status != "dismissed")
    stmt = stmt.order_by(NewsletterConfirmation.processed_at.desc()).limit(limit)
    rows = session.scalars(stmt).all()
    return [
        {
            "id": str(r.id),
            "sender_name": r.sender_name,
            "sender_email": r.sender_email,
            "subject": r.subject,
            "confirmation_url": r.confirmation_url,
            "status": r.status,
            "status_detail": r.status_detail,
            "received_at": r.received_at.isoformat() if r.received_at else None,
            "processed_at": r.processed_at.isoformat() if r.processed_at else None,
        }
        for r in rows
    ]


def dismiss_newsletter_confirmation(session: Session, user_id: UUID, conf_id: UUID) -> dict[str, Any]:
    row = session.scalar(
        select(NewsletterConfirmation).where(
            NewsletterConfirmation.id == conf_id,
            NewsletterConfirmation.user_id == user_id,
        )
    )
    if row is None:
        raise ValueError(f"Confirmation {conf_id} introuvable")
    row.status = "dismissed"
    row.processed_at = utcnow()
    session.commit()
    return {"id": str(row.id), "status": row.status}


def _complete_truncated_medium_title(raw_title: str, canonical_url: str, email_subject: str = "") -> str:
    """Complète un titre d'article Medium tronqué par '…' dans l'email à partir du Subject
    (pour l'article vedette) ou du slug de l'URL Medium.
    """
    t = (raw_title or "").strip()
    if not t.endswith(("…", "...")):
        return t

    prefix = re.sub(r"(?:…|\.\.\.)+$", "", t).strip()
    if email_subject:
        subj_main = email_subject.split("|")[0].strip()
        if len(subj_main) > len(prefix) and subj_main.lower().startswith(prefix[:28].lower()):
            return subj_main

    # Sinon, reconstruit la fin du titre depuis le slug d'URL (sans l'ID hexadécimal final)
    path = canonical_url.split("?")[0].rstrip("/")
    slug = path.split("/")[-1]
    slug_no_id = re.sub(r"-[0-9a-f]{8,12}$", "", slug)
    slug_words = [w for w in slug_no_id.split("-") if w]
    norm_prefix = re.sub(r"['’`]", "", prefix.lower())
    prefix_words = re.findall(r"[a-z0-9]+", norm_prefix)
    if prefix_words and len(slug_words) > len(prefix_words):
        tail_words = slug_words[len(prefix_words) :]
        if tail_words:
            tail_str = " ".join(tail_words)
            # Si le dernier mot du préfixe était coupé au milieu, remplace le dernier fragment
            last_pref = prefix_words[-1]
            corresponding_slug_word = slug_words[len(prefix_words) - 1]
            if corresponding_slug_word.startswith(last_pref) and len(corresponding_slug_word) > len(last_pref):
                prefix = re.sub(r"\S+$", corresponding_slug_word.capitalize(), prefix)
            return f"{prefix} {tail_str}".strip()
    return prefix


def _extract_medium_digest_links(
    soup: BeautifulSoup,
    newsletter_title: str,
    email_subject: str = "",
) -> list[dict[str, str]]:
    """Extracteur spécialisé pour le Medium Daily Digest :
    - Ne retient que les vraies URLs d'articles Medium (terminées par `-[0-9a-f]{8,12}`).
    - Exclut les profils d'auteurs, pages de publications, liens d'aide/politique et l'autopromo `@dailynewsletter`.
    - Sépare proprement le titre (`h2`/`h3`) du sous-titre (`p`/`div`) et récupère l'auteur / temps de lecture.
    """
    seen_urls: set[str] = set()
    extracted: list[dict[str, str]] = []

    for a_tag in soup.find_all("a", href=True):
        href = (a_tag.get("href") or "").strip()
        if "medium.com" not in href.lower():
            continue
        path = href.split("?")[0].rstrip("/")
        path_low = path.lower()
        if any(
            skip in path_low
            for skip in (
                "policy.medium.com",
                "help.medium.com",
                "jobs-at-medium",
                "/@dailynewsletter/",
                "/me/following",
                "/plans",
            )
        ):
            continue
        if not re.search(r"-[0-9a-f]{8,12}$", path_low):
            continue

        norm_url = canonicalize_url(href)
        if norm_url in seen_urls:
            continue

        children: list[str] = []
        for el in a_tag.find_all(["h1", "h2", "h3", "h4", "p", "div", "span"]):
            txt = " ".join(el.get_text(" ", strip=True).split())
            if txt and txt not in children:
                children.append(txt)

        full_txt = " ".join(a_tag.get_text(" ", strip=True).split())
        if len(full_txt) < 15:
            continue

        raw_title = children[0] if children else full_txt
        raw_subtitle = children[1] if len(children) > 1 else ""
        clean_title = _complete_truncated_medium_title(raw_title, norm_url, email_subject=email_subject)

        # Recherche l'auteur, la publication et le temps de lecture dans la carte parente
        author_meta: list[str] = []
        card_node = a_tag
        for _ in range(4):
            if card_node.parent is not None:
                card_node = card_node.parent
        if card_node is not None:
            for s_piece in card_node.stripped_strings:
                piece = " ".join(s_piece.split()).strip()
                if not piece or piece in (raw_title, raw_subtitle, full_txt) or piece.isdigit():
                    continue
                if piece in ("·", "in", "Member-only", "Member only"):
                    continue
                if piece not in author_meta and len(piece) <= 65:
                    author_meta.append(piece)

        meta_suffix = f" ({' · '.join(author_meta[:3])})" if author_meta else ""
        if raw_subtitle:
            curator_note = f"{raw_subtitle}{meta_suffix}"
        else:
            curator_note = f"{clean_title}{meta_suffix} — Sélection Medium Daily Digest"

        seen_urls.add(norm_url)
        extracted.append(
            {
                "title": clean_title[:280],
                "url": norm_url,
                "canonical_url": norm_url,
                "curator_note": curator_note[:600],
            }
        )
        if len(extracted) >= 15:
            break

    return extracted


def extract_curated_links_from_html(
    html_body: str,
    newsletter_title: str,
    sender_email: str = "",
    email_subject: str = "",
) -> list[dict[str, str]]:
    """Extrait les liens éditoriaux unitaires d'une newsletter de curation HTML en excluant
    l'autopromotion du service éditeur et les blocs sponsorisés.
    """
    if not html_body:
        return []
    soup = BeautifulSoup(html_body, "lxml")
    for tag in soup(["script", "style", "head", "footer", "nav"]):
        tag.decompose()

    if (
        "medium.com" in (sender_email or "").lower()
        or "medium daily digest" in (newsletter_title or "").lower()
        or "digest.reader" in html_body[:20000]
    ):
        medium_links = _extract_medium_digest_links(soup, newsletter_title, email_subject=email_subject)
        if medium_links:
            return medium_links

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
        "confirmations_auto_validated": 0,
        "confirmations_manual_required": 0,
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
            message_id = (msg.get("Message-ID") or f"imap-{num.decode()}").strip()[:500]

            # 0. Détection et validation automatique prudente des emails de confirmation d'inscription (double opt-in)
            if is_newsletter_optin_confirmation_email(msg, html_body, text_body):
                existing_conf = session.scalar(
                    select(NewsletterConfirmation).where(
                        NewsletterConfirmation.user_id == user_id,
                        NewsletterConfirmation.message_id == message_id,
                    )
                )
                if existing_conf is None:
                    unwrapped_conf = unwrap_forwarded_email_metadata(msg, html_body, text_body)
                    conf_url = extract_newsletter_confirmation_url(html_body, text_body)
                    if conf_url:
                        val_res = validate_newsletter_confirmation_url(conf_url)
                        conf_status = val_res["status"]
                        final_conf_url = val_res.get("final_url") or conf_url
                        session.add(
                            NewsletterConfirmation(
                                user_id=user_id,
                                message_id=message_id,
                                sender_name=unwrapped_conf["sender_name"][:250],
                                sender_email=unwrapped_conf["sender_email"][:315],
                                subject=unwrapped_conf["subject"][:500],
                                confirmation_url=(conf_url if conf_status == "manual_required" else final_conf_url)[:2040],
                                status=conf_status,
                                status_detail=(val_res.get("detail") or "")[:500],
                                received_at=unwrapped_conf["published_at"],
                                processed_at=utcnow(),
                            )
                        )
                        if conf_status == "confirmed":
                            stats["confirmations_auto_validated"] += 1
                            list_id_conf = decode_mime_words(msg.get("List-Id", "")) or None
                            src_conf, _ = register_discovered_newsletter(
                                session,
                                user_id=user_id,
                                sender_name=unwrapped_conf["sender_name"],
                                sender_email=unwrapped_conf["sender_email"],
                                list_id=list_id_conf,
                                subject_hint=unwrapped_conf["subject"],
                            )
                            stats["newsletter_sources_updated"].add(src_conf.title)
                        else:
                            stats["confirmations_manual_required"] += 1
                        session.flush()
                continue

            is_nl, detection_reason = is_newsletter_message(msg, html_body, text_body)
            if not is_nl:
                continue

            stats["newsletters_detected"] += 1

            unwrapped = unwrap_forwarded_email_metadata(msg, html_body, text_body)
            sender_name = unwrapped["sender_name"]
            sender_email = unwrapped["sender_email"]
            subject = unwrapped["subject"]
            pub_dt = unwrapped["published_at"]
            list_id = decode_mime_words(msg.get("List-Id", "")) or None

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
                email_subject=subject,
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

            for idx, link_info in enumerate(curated_links[:15]):
                link_url = link_info["url"]
                parsed_host = urlparse(link_url).netloc.lower()
                if not parsed_host:
                    continue

                item_title = " ".join(link_info["title"].split())
                item_excerpt = link_info["curator_note"]
                item_md = (
                    f"# {item_title}\n\n"
                    f"- **URL** : {link_info['canonical_url']}\n"
                    f"- **Via Newsletter** : {sender_name} (*{subject}*)\n\n"
                    f"## Note de curation\n\n{item_excerpt}\n"
                )

                # Pour les newsletters Pour la Science, résout les éventuels liens de redirection d'emailing
                # vers l'URL finale sur www.pourlascience.fr afin d'assurer la déduplication et l'enrichissement abonné
                is_pls_nl = "pourlascience" in sender_email.lower() or "pour la science" in sender_name.lower()
                if is_pls_nl and "pourlascience.fr" not in parsed_host:
                    try:
                        import httpx as _httpx

                        with _httpx.Client(follow_redirects=True, timeout=6.0) as _hc:
                            _r = _hc.get(link_url)
                            final_u = str(_r.url)
                            if "pourlascience.fr" in final_u:
                                link_url = final_u
                                link_info["canonical_url"] = canonicalize_url(final_u)
                                parsed_host = urlparse(link_url).netloc.lower()
                    except Exception:
                        pass

                if "pourlascience.fr" in parsed_host:
                    is_promo_pls, _ = is_promotional_or_self_promo_link(
                        raw_url=link_url,
                        canonical_url=link_info["canonical_url"],
                        title=item_title,
                        context_text=item_excerpt,
                        source_title=sender_name,
                    )
                    if is_promo_pls or "boutique." in parsed_host or "sso.qiota.com" in parsed_host:
                        continue

                # Enrichissement automatique abonné (Medium / Pour la Science) dès la réception de la newsletter
                if "medium.com" in parsed_host or "pourlascience.fr" in parsed_host:
                    try:
                        from morning_review.connectors.mcp_enricher import (
                            _detect_mcp_tool,
                            _fetch_via_agent_auth_mcp,
                            _fetch_via_trafilatura,
                            _get_runtime_token_path,
                        )
                        from morning_review.pipeline.synthesizer import _clean_markdown_body_paragraphs

                        tool_name, client_spec = _detect_mcp_tool(link_info["canonical_url"])
                        fetched_md = ""
                        if tool_name and client_spec and _get_runtime_token_path(tool_name).exists():
                            try:
                                mcp_res = _fetch_via_agent_auth_mcp(client_spec, link_info["canonical_url"])
                                fetched_md = (
                                    mcp_res.get("markdown")
                                    or mcp_res.get("markdown_body")
                                    or mcp_res.get("content")
                                    or ""
                                ).strip()
                                if mcp_res.get("title") and len(str(mcp_res["title"]).strip()) > 12:
                                    item_title = str(mcp_res["title"]).strip()[:280]
                            except Exception:
                                fetched_md = ""

                        if not fetched_md and ("…" in item_excerpt or len(item_excerpt) < 160):
                            fetched = _fetch_via_trafilatura(link_info["canonical_url"])
                            fetched_md = (fetched.get("markdown") or "").strip()
                            if fetched_md:
                                h1_match = re.search(r"^#\s+(.+)$", fetched_md, flags=re.MULTILINE)
                                if h1_match and len(h1_match.group(1).strip()) > 15:
                                    item_title = h1_match.group(1).strip()[:280]

                        if fetched_md:
                            paras = _clean_markdown_body_paragraphs(fetched_md)
                            if paras:
                                rich_summary = " ".join(paras[:3])[:650].strip()
                                if len(rich_summary) > len(item_excerpt):
                                    item_excerpt = rich_summary
                            item_md = (
                                f"# {item_title}\n\n"
                                f"- **URL** : {link_info['canonical_url']}\n"
                                f"- **Via Newsletter** : {sender_name} (*{subject}*)\n\n"
                                f"---\n\n"
                                f"{fetched_md}\n"
                            )
                    except Exception as exc:
                        logger.debug("Newsletter link auto-enrich skipped for %s: %s", link_url, exc)

                _, link_topic, link_created = ingest_content_item(
                    session,
                    user_id=user_id,
                    source=source,
                    raw_url=link_url,
                    title=item_title,
                    markdown_body=item_md,
                    excerpt=item_excerpt,
                    author=sender_name,
                    published_at=pub_dt,
                    external_id=f"link:{message_id}:{idx}:{link_info['canonical_url'][:120]}",
                    curator_note=item_excerpt,
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
                        "curated_links_extracted": len(curated_links[:15]),
                        "category": sub.category_slug,
                    }
                )

        session.commit()

    return {
        "scanned_messages": stats["scanned_messages"],
        "newsletters_detected": stats["newsletters_detected"],
        "confirmations_auto_validated": stats["confirmations_auto_validated"],
        "confirmations_manual_required": stats["confirmations_manual_required"],
        "newsletter_sources_count": len(stats["newsletter_sources_updated"]),
        "newsletter_sources": sorted(stats["newsletter_sources_updated"]),
        "items_ingested": stats["items_ingested"],
        "unique_topics_touched": len(stats["topics_touched"]),
        "sample_newsletters": stats["sample_newsletters"],
    }
