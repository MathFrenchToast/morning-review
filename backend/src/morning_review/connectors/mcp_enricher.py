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


def _run_agent_auth_mcp_script(code: str, timeout: int = 35) -> Any:
    """Exécute un snippet Python en lecture seule dans l'environnement `agent-auth-mcp` et décode la sortie JSON."""
    env = {**os.environ, "PYTHONPATH": str(AGENT_AUTH_MCP_DIR)}
    proc = subprocess.run(
        ["uv", "run", "python", "-c", code],
        cwd=str(AGENT_AUTH_MCP_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"agent-auth-mcp error: {proc.stderr.strip() or proc.stdout.strip()}")
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    if not lines:
        return {}
    return json.loads(lines[-1])


def _fetch_via_agent_auth_mcp(client_spec: str, url: str) -> dict[str, Any]:
    """Appelle en lecture seule le client correspondant du projet frère agent-auth-mcp."""
    module_name, class_name = client_spec.split(":")
    code = (
        f"import json\n"
        f"from {module_name} import {class_name}\n"
        f"with {class_name}() as c:\n"
        f"    res = c.read_article({url!r})\n"
        f"print(json.dumps(res, ensure_ascii=False))\n"
    )
    res = _run_agent_auth_mcp_script(code, timeout=30)
    return res if isinstance(res, dict) else {}


def list_pourlascience_latest_articles(category: str = "", limit: int = 20) -> list[dict[str, Any]]:
    """Liste les derniers articles publiés sur `www.pourlascience.fr` (accueil ou rubrique thématique)."""
    code = (
        "import json\n"
        "from tools.pourlascience.client import PourLaScienceClient\n"
        "with PourLaScienceClient() as c:\n"
        f"    res = c.list_latest_articles(category={category!r}, limit={int(limit)})\n"
        "print(json.dumps(res, ensure_ascii=False))\n"
    )
    res = _run_agent_auth_mcp_script(code, timeout=30)
    return res if isinstance(res, list) else []


def list_pourlascience_issues(
    source: str = "all",
    year: int | None = None,
    issue_number: int | None = None,
    query: str = "",
    page: int = 1,
    limit: int = 24,
) -> list[dict[str, Any]]:
    """Liste les numéros mensuels (`pls`), Hors-Séries (`dpls`) ou l'ensemble des archives (`all`) de Pour la Science."""
    code = (
        "import json\n"
        "from tools.pourlascience.client import PourLaScienceClient\n"
        "with PourLaScienceClient() as c:\n"
        f"    res = c.list_issues(source={source!r}, year={year!r}, issue_number={issue_number!r}, query={query!r}, page={int(page)}, limit={int(limit)})\n"
        "print(json.dumps(res, ensure_ascii=False))\n"
    )
    res = _run_agent_auth_mcp_script(code, timeout=35)
    return res if isinstance(res, list) else []


def get_pourlascience_issue_articles(issue_url_or_number: str) -> dict[str, Any]:
    """Récupère le sommaire complet et les articles d'un numéro ou Hors-Série Pour la Science (ex: '589', 'HS133' ou URL)."""
    code = (
        "import json\n"
        "from tools.pourlascience.client import PourLaScienceClient\n"
        "with PourLaScienceClient() as c:\n"
        f"    res = c.get_issue_articles({issue_url_or_number!r})\n"
        "print(json.dumps(res, ensure_ascii=False))\n"
    )
    res = _run_agent_auth_mcp_script(code, timeout=35)
    return res if isinstance(res, dict) else {}


def search_pourlascience_articles(query: str, page: int = 1, limit: int = 20) -> list[dict[str, Any]]:
    """Recherche dans tous les articles et archives du site `www.pourlascience.fr`."""
    code = (
        "import json\n"
        "from tools.pourlascience.client import PourLaScienceClient\n"
        "with PourLaScienceClient() as c:\n"
        f"    res = c.search_articles(query={query!r}, page={int(page)}, limit={int(limit)})\n"
        "print(json.dumps(res, ensure_ascii=False))\n"
    )
    res = _run_agent_auth_mcp_script(code, timeout=35)
    return res if isinstance(res, list) else []


def _ensure_www_if_needed(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host in ("pourlascience.fr", "newscientist.com", "quantamagazine.org"):
        return urlunparse(parsed._replace(netloc=f"www.{host}"))
    return url


def _extract_jsonld_or_meta_description(raw_html: str) -> str:
    """Extrait `articleBody` ou `description` depuis les blocs JSON-LD ou les balises meta
    lorsque Trafilatura renvoie peu de texte (ex: pages YouTube, podcasts ou SPA).
    """
    import html as html_lib
    import re

    pieces: list[str] = []
    for match in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        raw_html,
        flags=re.DOTALL | re.IGNORECASE,
    ):
        raw_json = match.group(1).strip()
        try:
            data = json.loads(raw_json)
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                for key in ("articleBody", "description", "text"):
                    val = obj.get(key)
                    if isinstance(val, str) and len(val.strip()) > 80:
                        pieces.append(html_lib.unescape(val.strip()))
        except Exception:
            continue

    if not pieces:
        # Tentative pour la description complète YouTube dans ytInitialPlayerResponse
        yt_match = re.search(r'"shortDescription":"((?:\\.|[^"\\])*)"', raw_html)
        if yt_match:
            try:
                desc = json.loads(f'"{yt_match.group(1)}"')
                if len(desc.strip()) > 60:
                    pieces.append(desc.strip())
            except Exception:
                pass

    if not pieces:
        meta_match = re.search(
            r'<meta[^>]+(?:property|name)=["\'](?:og:description|description)["\'][^>]+content=["\']([^"\']+)["\']',
            raw_html,
            flags=re.IGNORECASE,
        )
        if meta_match:
            val = html_lib.unescape(meta_match.group(1).strip())
            if len(val) > 60:
                pieces.append(val)

    return "\n\n".join(dict.fromkeys(pieces))


def _fetch_via_trafilatura(url: str) -> dict[str, Any]:
    """Extraction publique déterministe via httpx + Trafilatura (mode favor_recall=True)."""
    target = _ensure_www_if_needed(url)
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    raw_html = ""
    try:
        with httpx.Client(follow_redirects=True, timeout=12.0, headers=headers) as client:
            resp = client.get(target)
            if resp.status_code < 400:
                raw_html = resp.text
    except Exception as exc:
        logger.debug("httpx fetch fallback for %s: %s", target, exc)

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
            include_tables=True,
            include_comments=False,
            favor_recall=True,
        )
        or ""
    ).strip()

    extra_structured = _extract_jsonld_or_meta_description(raw_html)
    if len(extra_structured) > len(md_text):
        md_text = f"{extra_structured}\n\n{md_text}".strip() if md_text else extra_structured

    lower = (md_text + " " + raw_html[:8000]).lower()
    paywall_markers = (
        "cet article est réservé aux abonnés",
        "il vous reste",
        "subscribe to read",
        "already a subscriber",
        "pour lire la suite, abonnez-vous",
        "become a member to read",
        "member-only story",
    )
    paywall_detected = any(m in lower for m in paywall_markers) or len(md_text.strip()) < 500
    return {
        "url": target,
        "markdown": md_text.strip(),
        "markdown_body": md_text.strip(),
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
            mcp_md = (
                article_data.get("markdown")
                or article_data.get("markdown_body")
                or article_data.get("content")
                or ""
            )
            if len(mcp_md.strip()) < 200:
                article_data = _fetch_via_trafilatura(target_url)
                enrichment_method = "trafilatura_public"
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

    markdown_content = (
        article_data.get("markdown")
        or article_data.get("markdown_body")
        or article_data.get("content")
        or ""
    )
    paywall_detected = bool(
        article_data.get(
            "paywall_detected",
            article_data.get("is_paywalled_or_truncated", False),
        )
    )

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

    # Mise à jour du résumé du Topic et du ContentItem correspondant à target_url
    from morning_review.pipeline.ingestion import canonicalize_url
    from morning_review.pipeline.synthesizer import (
        _clean_markdown_body_paragraphs,
        synthesize_topic_summary,
    )

    clean_paras = _clean_markdown_body_paragraphs(markdown_content)
    if clean_paras:
        rich_excerpt = " ".join(clean_paras[:4])[:650]
    else:
        clean_lines = [
            line.strip()
            for line in markdown_content.splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        rich_excerpt = " ".join(clean_lines)[:600]

    norm_target = canonicalize_url(target_url)
    item = session.scalar(
        select(CanonicalItem).where(
            CanonicalItem.topic_id == topic.id,
            CanonicalItem.canonical_url == norm_target,
        )
    )
    if item is None:
        item = session.scalar(
            select(CanonicalItem)
            .where(CanonicalItem.topic_id == topic.id)
            .order_by(CanonicalItem.published_at.asc())
        )
    if item is not None:
        item.s3_key = s3_key
        item.content_sha256 = sha256_hex
        if rich_excerpt:
            item.excerpt = rich_excerpt[:500]

    session.flush()
    synthesize_topic_summary(session, topic)
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
