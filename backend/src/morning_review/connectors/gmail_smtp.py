from __future__ import annotations

from datetime import date, datetime
from email.message import EmailMessage
from email.utils import format_datetime, formataddr, make_msgid
import html
import smtplib
import ssl
from typing import Any, Callable
from urllib.parse import urlparse

from morning_review.core.config import settings


def is_public_web_url(url: str) -> bool:
    """Retourne True uniquement si l'URL pointe vers un hôte public (exclut localhost/loopback/IP privées)
    afin de ne jamais déclencher les filtres anti-phishing webmail sur des liens `http://localhost`.
    """
    if not url:
        return False
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").lower().strip()
    if not host or host in ("localhost", "127.0.0.1", "0.0.0.0", "::1") or host.endswith(".local"):
        return False
    if host.startswith(("127.", "10.", "192.168.")):
        return False
    return "." in host


def canonicalize_gmail_sender(account: str) -> str:
    """Évite la réécriture forcée `X-Google-Original-From` par `smtp.gmail.com` lorsque le compte
    Google canonique ne comporte pas de point dans la partie locale alors que `.env` en contient un.
    """
    addr = account.strip()
    if "@" not in addr:
        return addr
    local_part, domain = addr.rsplit("@", 1)
    if domain.lower() == "gmail.com" and "." in local_part:
        return f"{local_part.replace('.', '')}@{domain.lower()}"
    return addr


def build_digest_plain_text(
    *,
    target_date: date,
    by_category: dict[str, list[dict[str, Any]]],
    confirmations: list[dict[str, Any]],
    ui_url: str,
) -> str:
    """Construit la version `text/plain` propre et alignée sur le `text/html` (sans YAML frontmatter ni UUIDs)."""
    total_topics = sum(len(v) for v in by_category.values())
    updates_count = sum(1 for v in by_category.values() for item in v if item.get("has_updates"))
    public_ui = ui_url if is_public_web_url(ui_url) else ""

    lines: list[str] = [
        f"Morning Review — {target_date.isoformat()}",
        f"{total_topics} sujet(s) actif(s)"
        + (f" | {updates_count} mise(s) à jour" if updates_count else ""),
    ]
    if public_ui:
        lines.append(f"Interface Web : {public_ui}")
    lines.append("")

    if confirmations:
        lines.append("Inscriptions Newsletters (Double Opt-In) :")
        for c in confirmations:
            sname = c.get("sender_name") or ""
            subj = c.get("subject") or ""
            curl = c.get("confirmation_url") or ""
            if c.get("status") == "confirmed":
                lines.append(f"  - [Confirmée] {sname} : {subj}")
            else:
                lines.append(f"  - [Action requise] {sname} ({subj}) : {curl}")
        lines.append("")

    for cat_slug, items in sorted(by_category.items()):
        lines.append(f"=== {cat_slug.upper()} ({len(items)}) ===")
        lines.append("")
        for item in items:
            title = item.get("title") or "Sans titre"
            purl = item.get("primary_url") or ""
            summary = item.get("summary") or ""
            src_cnt = item.get("source_count", 1)
            prefix = "[MISE À JOUR] " if item.get("has_updates") else ""
            lines.append(f"* {prefix}{title} ({src_cnt} source(s))")
            if item.get("has_updates") and item.get("updates_summary"):
                lines.append(f"  Nouveauté : {item['updates_summary']}")
            if summary:
                lines.append(f"  {summary}")
            for st in (item.get("subtopics") or [])[:3]:
                st_lbl = st.get("title") or st.get("label") or ""
                st_sum = st.get("summary") or ""
                if st_lbl and st_sum:
                    lines.append(f"    - {st_lbl} : {st_sum}")
            if purl and is_public_web_url(purl):
                lines.append(f"  Lien : {purl}")
            lines.append("")

    return "\n".join(lines).strip() + "\n"


def build_digest_html(
    *,
    target_date: date,
    by_category: dict[str, list[dict[str, Any]]],
    confirmations: list[dict[str, Any]],
    ui_url: str,
) -> str:
    """Construit la version HTML responsive (CSS inline compatible Gmail Web/Mobile) du Morning Review,
    sans aucun lien vers `localhost` et avec une densité de liens sortants maîtrisée.
    """
    total_topics = sum(len(v) for v in by_category.values())
    updates_count = sum(1 for v in by_category.values() for item in v if item.get("has_updates"))
    has_public_ui = is_public_web_url(ui_url)
    esc_ui = html.escape(ui_url, quote=True) if has_public_ui else ""

    header_cta = (
        f'<a href="{esc_ui}" style="display:inline-block;background:#2563eb;color:#ffffff;text-decoration:none;font-size:13px;font-weight:600;padding:8px 14px;border-radius:8px;">Ouvrir l\'interface</a>'
        if has_public_ui
        else ""
    )

    parts: list[str] = [
        "<!DOCTYPE html>",
        '<html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">',
        f"<title>Morning Review — {target_date.isoformat()}</title></head>",
        '<body style="margin:0;padding:0;background-color:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Roboto,Helvetica,Arial,sans-serif;color:#111827;line-height:1.55;">',
        '<div style="max-width:720px;margin:0 auto;padding:24px 14px;">',
        '<div style="background:#0f172a;color:#f8fafc;border-radius:12px;padding:22px 24px;margin-bottom:20px;">',
        '<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;">',
        "<div>",
        f'<h1 style="margin:0;font-size:21px;font-weight:700;letter-spacing:-0.01em;">Morning Review — {target_date.isoformat()}</h1>',
        f'<p style="margin:6px 0 0 0;font-size:13px;color:#cbd5e1;"><strong>{total_topics}</strong> sujet(s) actif(s)'
        + (f' • <strong>{updates_count}</strong> mise(s) à jour détectée(s)' if updates_count else "")
        + "</p>",
        "</div>",
        header_cta,
        "</div></div>",
    ]

    if confirmations:
        parts.append(
            '<div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:10px;padding:16px 18px;margin-bottom:20px;">'
            '<h2 style="margin:0 0 10px 0;font-size:15px;color:#1e3a8a;">Inscriptions Newsletters (Double Opt-In)</h2>'
            '<ul style="margin:0;padding-left:18px;font-size:13px;color:#1e293b;">'
        )
        for c in confirmations:
            sname = html.escape(c.get("sender_name") or "")
            subj = html.escape(c.get("subject") or "")
            curl = c.get("confirmation_url") or ""
            esc_curl = html.escape(curl, quote=True)
            if c.get("status") == "confirmed":
                parts.append(
                    f'<li style="margin-bottom:6px;"><strong>Confirmée automatiquement</strong> : '
                    f"<strong>{sname}</strong> — <em>{subj}</em></li>"
                )
            elif is_public_web_url(curl):
                parts.append(
                    f'<li style="margin-bottom:8px;"><strong>Validation 1-clic requise</strong> : '
                    f"<strong>{sname}</strong> — "
                    f'<a href="{esc_curl}" style="color:#1d4ed8;font-weight:600;">Confirmer l\'inscription</a></li>'
                )
        parts.append("</ul></div>")

    if not by_category:
        parts.append(
            '<div style="background:#ffffff;border-radius:10px;padding:20px;border:1px solid #e5e7eb;text-align:center;color:#6b7280;">'
            "Aucun nouveau sujet non archivé sur cette période."
            "</div>"
        )

    for cat_slug, items in sorted(by_category.items()):
        esc_cat = html.escape(cat_slug.upper())
        parts.append(
            f'<div style="margin:22px 0 10px 0;padding-bottom:6px;border-bottom:2px solid #cbd5e1;">'
            f'<span style="font-size:12px;font-weight:700;letter-spacing:0.06em;color:#334155;background:#e2e8f0;padding:3px 9px;border-radius:999px;">{esc_cat}</span> '
            f'<span style="font-size:12px;color:#64748b;margin-left:6px;">{len(items)} sujet(s)</span>'
            f"</div>"
        )
        for item in items:
            title = html.escape(item.get("title") or "Sans titre")
            raw_purl = item.get("primary_url") or ""
            has_valid_purl = is_public_web_url(raw_purl)
            purl = html.escape(raw_purl, quote=True) if has_valid_purl else ""
            summary = html.escape(item.get("summary") or "")
            score = item.get("relevance_score", 0)
            src_cnt = item.get("source_count", 1)
            has_upd = bool(item.get("has_updates"))
            upd_sum = html.escape(item.get("updates_summary") or "")

            parts.append(
                '<div style="background:#ffffff;border:1px solid #e5e7eb;border-radius:10px;padding:16px 18px;margin-bottom:12px;">'
            )
            if has_upd:
                parts.append(
                    '<div style="margin-bottom:8px;">'
                    '<span style="display:inline-block;background:#fef3c7;color:#92400e;font-size:11px;font-weight:700;padding:2px 8px;border-radius:999px;">MISE À JOUR</span>'
                    "</div>"
                )
            title_html = (
                f'<a href="{purl}" style="color:#0f172a;text-decoration:none;">{title}</a>'
                if has_valid_purl
                else title
            )
            sources = item.get("sources") or []
            src_names = ", ".join(dict.fromkeys(html.escape(s.get("source_title") or "Source") for s in sources[:5]))
            parts.append(
                f'<h3 style="margin:0 0 6px 0;font-size:16px;line-height:1.35;">{title_html}</h3>'
                f'<div style="font-size:12px;color:#64748b;margin-bottom:8px;">'
                f"Pertinence : <strong>{score}</strong> • <strong>{src_cnt}</strong> source(s)"
                + (f" ({src_names})" if src_names else "")
                + "</div>"
            )
            if has_upd and upd_sum:
                parts.append(
                    f'<div style="background:#fffbeb;border-left:3px solid #f59e0b;padding:8px 12px;margin-bottom:10px;font-size:13px;color:#78350f;">'
                    f"<strong>Nouveauté :</strong> {upd_sum}"
                    f"</div>"
                )
            if summary:
                parts.append(f'<p style="margin:0 0 10px 0;font-size:14px;color:#334155;">{summary}</p>')

            subtopics = item.get("subtopics") or []
            if subtopics:
                parts.append('<ul style="margin:0 0 8px 0;padding-left:18px;font-size:13px;color:#475569;">')
                for st in subtopics[:3]:
                    st_lbl = html.escape(st.get("title") or st.get("label") or "")
                    st_sum = html.escape(st.get("summary") or "")
                    if st_lbl and st_sum:
                        parts.append(f'<li style="margin-bottom:4px;"><strong>{st_lbl}</strong> : {st_sum}</li>')
                    elif st_sum or st_lbl:
                        parts.append(f'<li style="margin-bottom:4px;">{st_sum or st_lbl}</li>')
                parts.append("</ul>")

            parts.append("</div>")

    footer_html = (
        f'Morning Review • <a href="{esc_ui}" style="color:#64748b;">{esc_ui}</a>'
        if has_public_ui
        else "Synthèse quotidienne générée par Morning Review"
    )
    parts.append(
        f'<div style="text-align:center;font-size:12px;color:#94a3b8;margin-top:24px;">{footer_html}</div>'
        "</div></body></html>"
    )
    return "\n".join(parts)


def send_digest_email(
    *,
    subject: str,
    plain_markdown: str,
    html_body: str,
    recipient_email: str | None = None,
    smtp_factory: Callable[[], Any] | None = None,
) -> dict[str, str]:
    """Expédie le digest quotidien via Gmail SMTP_SSL (port 465) avec en-têtes RFC 5322 complets."""
    sender_account = settings.gmail_account.strip()
    raw_pwd = settings.gmail_pwd.strip()
    if not sender_account or not raw_pwd:
        raise RuntimeError("GMAIL_ACCOUNT ou GMAIL_PWD manquant dans .env pour l'envoi SMTP")

    password = (
        raw_pwd.replace(" ", "")
        if len(raw_pwd.replace(" ", "")) == 16 and raw_pwd.replace(" ", "").isalpha()
        else raw_pwd
    )
    target_to = (recipient_email or settings.digest_recipient_email).strip()
    if not target_to:
        raise RuntimeError("DIGEST_RECIPIENT_EMAIL manquant")

    canonical_from = canonicalize_gmail_sender(sender_account)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr(("Morning Review", canonical_from))
    msg["To"] = target_to
    msg["Date"] = format_datetime(datetime.now().astimezone())
    msg["Message-ID"] = make_msgid(domain="gmail.com")
    msg.set_content(plain_markdown)
    msg.add_alternative(html_body, subtype="html")

    if smtp_factory is not None:
        with smtp_factory() as server:
            server.login(canonical_from, password)
            server.send_message(msg)
    else:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(
            settings.gmail_smtp_host,
            settings.gmail_smtp_port,
            local_hostname="[127.0.0.1]",
            context=context,
            timeout=25,
        ) as server:
            server.login(canonical_from, password)
            server.send_message(msg)

    return {
        "from": canonical_from,
        "to": target_to,
        "subject": subject,
    }
