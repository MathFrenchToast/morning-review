from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse
from uuid import UUID

import httpx
import trafilatura
from sqlalchemy import select
from sqlalchemy.orm import Session

from morning_review.core.storage import storage
from morning_review.db.models import CanonicalItem, Topic

logger = logging.getLogger(__name__)

AGENT_AUTH_MCP_DIR = Path(
    os.environ.get(
        "AGENT_AUTH_MCP_DIR",
        "/home/mathieu/dev/projects/perso/agent-auth-mcp",
    )
)

DOMAIN_TO_MCP_TOOL: dict[str, tuple[str, str]] = {
    "newscientist.com": ("newscientist", "tools.newscientist.client:NewScientistClient"),
    "pourlascience.fr": ("pourlascience", "tools.pourlascience.client:PourLaScienceClient"),
    "medium.com": ("medium", "tools.medium.client:MediumClient"),
}


def _detect_mcp_tool(url: str) -> tuple[str | None, str | None]:
    host = (urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    for domain, (tool_name, client_spec) in DOMAIN_TO_MCP_TOOL.items():
        if host == domain or host.endswith(f".{domain}"):
            return tool_name, client_spec
    return None, None


def _get_runtime_token_path(tool_name: str) -> Path:
    uid = os.getuid()
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{uid}"
    return Path(runtime_dir) / "agent-tokens" / f"{tool_name}.token"


def _fetch_via_agent_auth_mcp(client_spec: str, url: str) -> dict[str, Any]:
    """Appelle en lecture seule le client correspondant du projet frère agent-auth-mcp."""
    module_name, class_name = client_spec.split(":")
    code = (
        f"import json; "
        f"from {module_name} import {class_name}; "
        f"with {class_name}() as c: "
        f"    res = c.read_article({url!r}); "
        f"print(json.dumps(res, ensure_ascii=False))"
    )
    proc = subprocess.run(
        ["uv", "run", "python", "-c", code],
        cwd=str(AGENT_AUTH_MCP_DIR),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"agent-auth-mcp error: {proc.stderr.strip() or proc.stdout.strip()}")
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _ensure_www_if_needed(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host in ("pourlascience.fr", "newscientist.com", "quantamagazine.org"):
        return urlunparse(parsed._replace(netloc=f"www.{host}"))
    return url


def _fetch_via_trafilatura(url: str) -> dict[str, Any]:
    """Extraction publique déterministe via httpx + Trafilatura (pour Quanta Magazine ou aperçu public)."""
    target = _ensure_www_if_needed(url)
    headers = {
        "User-Agent": "MorningReview/1.0 (RSS Reader; Feed Fetcher)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    raw_html = ""
    with httpx.Client(follow_redirects=True, timeout=15.0, headers=headers) as client:
        resp = client.get(target)
        if resp.status_code < 400:
            raw_html = resp.text

    if not raw_html:
        raw_html = trafilatura.fetch_url(target) or ""
    if not raw_html:
        raise RuntimeError(f"Impossible de télécharger l'URL publique : {target}")

    md_text = (
        trafilatura.extract(
            raw_html,
            output_format="markdown",
            include_links=True,
            include_formatting=True,
        )
        or ""
    )
    lower = (md_text + " " + raw_html[:8000]).lower()
    paywall_markers = (
        "cet article est réservé aux abonnés",
        "il vous reste",
        "subscribe to read",
        "already a subscriber",
        "pour lire la suite, abonnez-vous",
        "become a member to read",
    )
    paywall_detected = any(m in lower for m in paywall_markers) or len(md_text.strip()) < 600
    return {
        "url": target,
        "markdown": md_text.strip(),
        "paywall_detected": paywall_detected,
    }


def enrich_topic_content(
    session: Session,
    *,
    user_id: UUID,
    topic_id: UUID,
) -> dict[str, Any]:
    """Étape 2 (Lazy Deep-Fetch) : enrichit à la demande un topic_id découvert par flux public.

    - Si l'URL correspond à un outil de `agent-auth-mcp` (`newscientist`, `pourlascience`, `medium`)
      et que le jeton `/run/user/$UID/agent-tokens/<tool>.token` est présent, utilise le MCP abonné.
    - Sinon, utilise `trafilatura` en mode public (ex: Quanta Magazine) et indique la commande
      d'authentification MCP si un paywall est détecté.
    """
    topic = session.get(Topic, topic_id)
    if topic is None:
        raise ValueError(f"Topic introuvable : {topic_id}")
    if topic.visibility != "public" and topic.user_id != user_id:
        raise ValueError("Accès refusé à ce topic privé")

    target_url = _ensure_www_if_needed(topic.primary_url or "")
    tool_name, client_spec = _detect_mcp_tool(target_url)
    token_path = _get_runtime_token_path(tool_name) if tool_name else None
    mcp_token_available = bool(
        tool_name
        and token_path
        and token_path.exists()
        and AGENT_AUTH_MCP_DIR.exists()
    )

    enrichment_method = "trafilatura_public"
    mcp_hint: str | None = None

    if mcp_token_available and client_spec and tool_name:
        try:
            article_data = _fetch_via_agent_auth_mcp(client_spec, target_url)
            enrichment_method = f"mcp_auth:{tool_name}"
        except Exception as exc:
            logger.warning("Fallback trafilatura après échec MCP (%s): %s", tool_name, exc)
            article_data = _fetch_via_trafilatura(target_url)
            mcp_hint = f"Erreur MCP ({exc}). Vérifiez le jeton : cd {AGENT_AUTH_MCP_DIR} && uv run python -m tools.{tool_name}.auth check"
    else:
        article_data = _fetch_via_trafilatura(target_url)
        if tool_name:
            mcp_hint = (
                f"Aucun jeton actif dans {token_path}. Pour débloquer le texte abonné complet via MCP : "
                f"cd {AGENT_AUTH_MCP_DIR} && uv run python -m tools.{tool_name}.auth login"
            )

    markdown_content = article_data.get("markdown") or ""
    paywall_detected = bool(article_data.get("paywall_detected", False))

    visibility = "private" if enrichment_method.startswith("mcp_auth:") else "public"
    full_md_doc = (
        f"# {topic.title}\n\n"
        f"- **URL** : {target_url}\n"
        f"- **Méthode d'enrichissement** : `{enrichment_method}`\n"
        f"- **Paywall détecté** : `{paywall_detected}`\n\n"
        f"---\n\n"
        f"{markdown_content}\n"
    )
    sha256_hex = storage.compute_sha256(full_md_doc)
    s3_key = storage.build_item_key(
        sha256_hex,
        visibility=visibility,
        user_id=user_id if visibility == "private" else None,
    )
    storage.put_markdown(s3_key, full_md_doc)

    # Mise à jour du résumé du Topic et du ContentItem associé si l'extraction est plus riche
    clean_lines = [
        line.strip()
        for line in markdown_content.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    rich_excerpt = " ".join(clean_lines)[:600]
    if len(rich_excerpt) > len(topic.summary or ""):
        topic.summary = rich_excerpt

    item = session.scalar(
        select(CanonicalItem)
        .where(CanonicalItem.topic_id == topic.id)
        .order_by(CanonicalItem.created_at.desc())
    )
    if item is not None:
        item.s3_key = s3_key
        item.content_sha256 = sha256_hex
        if len(rich_excerpt) > len(item.excerpt or ""):
            item.excerpt = rich_excerpt[:500]

    session.commit()

    return {
        "topic_id": str(topic.id),
        "title": topic.title,
        "url": target_url,
        "enrichment_method": enrichment_method,
        "mcp_tool": tool_name,
        "mcp_token_available": mcp_token_available,
        "paywall_detected": paywall_detected,
        "mcp_hint": mcp_hint,
        "s3_key": s3_key,
        "chars_extracted": len(markdown_content),
        "summary_preview": (topic.summary or "")[:300],
    }
