from __future__ import annotations

from datetime import datetime, timedelta
import re
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urlparse, urlunparse
from uuid import UUID

import numpy as np
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from morning_review.core.config import settings
from morning_review.core.storage import storage
from morning_review.db.models import (
    CanonicalItem,
    Source,
    SourceMention,
    Topic,
    TopicItem,
    UserTopicFeedback,
    UserTopicState,
    utcnow,
)
from morning_review.pipeline.embeddings import embedding_service

TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "mc_cid",
    "mc_eid",
    "fbclid",
    "gclid",
    "ref",
    "source",
    "s",
    "oly_enc_id",
    "oly_anon_id",
    "vero_id",
    "_hsenc",
    "_hsmi",
}

GENERIC_OR_STOPWORDS = {
    "le", "la", "les", "de", "des", "du", "un", "une", "et", "en", "au", "aux", "dans", "sur",
    "pour", "par", "avec", "sans", "ce", "que", "qui", "qu", "on", "est", "sont", "ont", "pas",
    "plus", "mais", "ou", "se", "sa", "son", "ses", "leur", "elle", "il", "ils", "elles", "nous",
    "vous", "the", "a", "an", "of", "to", "in", "on", "for", "with", "by", "from", "at", "is",
    "are", "was", "were", "be", "been", "being", "and", "or", "not", "but", "as", "if", "so",
    "how", "why", "what", "when", "where", "who", "after", "still", "new", "first", "time",
    "read", "minute", "minutes", "selon", "tout", "tous", "toute", "toutes", "sait", "touche",
    "monde", "mondiale", "global", "world", "france", "etat", "etats", "unis", "entre", "contre",
    "apres", "après", "plusieurs", "grand", "grande", "tres", "très", "avoir", "etre", "être",
    "faire", "comme", "cette", "cet", "ces", "chez", "vers", "encore", "ans", "jour", "jours",
    "can", "will", "could", "would", "should", "have", "has", "had", "this", "that", "these",
    "those", "into", "over", "under", "more", "most", "some", "any", "all", "out", "about",
    "your", "our", "their", "its", "you", "they", "we", "he", "she", "end", "user", "users",
    # Descripteurs génériques d'événement ou de format (exclus des entités nommées spécifiques)
    "panne", "outage", "down", "perturbation", "perturbations", "perturbees", "perturbes",
    "perturbées", "perturbés", "incident", "indisponible", "coupure", "blackout", "faille",
    "vulnerability", "vulnerabilities", "cve", "exploit", "zeroday", "breach", "cyberattaque",
    "ransomware", "flaw", "flaws", "bug", "bugs", "critical", "data", "records", "theft",
    "hack", "hackers", "attack", "attackers", "security", "server", "servers", "code", "cloud",
    "web", "linux", "ai", "model", "models", "agent", "agents", "open", "source", "tldr",
    "devops", "infosec", "dev", "pro", "explore", "edition", "guide", "guides", "started",
    "today", "hours", "github", "repo", "internet", "customer", "customers", "traffic",
    "company", "companies", "tech", "science", "news", "article", "podcast", "podcasts",
    # Jours, mois et termes génériques de grilles d'antenne (exclus des entités nommées)
    "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche",
    "janvier", "fevrier", "février", "mars", "avril", "mai", "juin", "juillet",
    "aout", "août", "septembre", "octobre", "novembre", "decembre", "décembre",
    "journal", "emission", "émission", "invite", "invité", "entretien", "chronique",
    "revue", "presse", "internationale", "episode", "épisode", "video", "vidéo",
    # Termes génériques de listicles, tutoriels et formats éditoriaux
    "top", "best", "tool", "tools", "outil", "outils", "list", "liste", "tips", "tricks",
    "ways", "things", "every", "need", "know", "use", "using", "build", "building",
    "intro", "introduction", "tutorial", "complet", "complete", "review", "overview",
    "comparison", "versus", "comment", "pourquoi", "quand", "quel", "quelle", "quels", "quelles",
    "home", "self", "hosted", "local", "modern", "simple", "fast", "free", "part",
    # Grands domaines académiques ou disciplinaires (catégories, non événements spécifiques)
    "physique", "physics", "quantique", "quantum", "chimie", "biologie", "mathématiques",
    "mathematiques", "maths", "histoire", "philosophie", "économie", "economie", "politique",
    "société", "societe", "culture", "numérique", "numerique", "informatique", "software",
    "hardware", "engineering", "architecture", "system", "systems", "système", "systèmes",
    "network", "networks", "réseau", "réseaux", "database", "databases", "infrastructure",
    " révolution", "revolution", "invisible", "devenu", "devient",
}

EVENT_CONCEPT_GROUPS = [
    {
        "panne", "outage", "down", "perturbation", "perturbations", "perturbees", "perturbes",
        "perturbées", "perturbés", "incident", "indisponible", "coupure", "blackout",
    },
    {
        "faille", "vulnerability", "vulnerabilities", "cve", "exploit", "zeroday", "breach",
        "cyberattaque", "ransomware", "flaw", "flaws",
    },
    {
        "scandale", "scandal", "scandals", "chute", "fall", "downfall", "affair", "story", "exit", "resign", "resigns",
        "resigned", "démission",
    },
]

SYNONYM_MAP = {
    "outlook": "microsoft",
    "msft365status": "microsoft",
    "microsoft365": "microsoft",
    "copilot": "microsoft",
    "teams": "microsoft",
    "broccoli2shell": "brotli",
    "http2shell": "brotli",
    "huang": "nvidia",
    "jensen": "nvidia",
}

TLDR_EDITORIAL_SUFFIX_RE = re.compile(
    r"\((?:\d+\s*minute\s*read|github\s*repo|website|paper|book|video|tool|podcast)\)\s*$",
    re.IGNORECASE,
)

DATED_ARTICLE_PATH_RE = re.compile(r"/(?:article/)?20\d{2}/\d{2}/", re.IGNORECASE)

EDITORIAL_ARTICLE_PATH_RE = re.compile(
    r"(?:/(?:p|blog|post|posts|article|articles|news|story|stories|sr|briefing)/|/20\d{2}/)",
    re.IGNORECASE,
)

GENERIC_ESP_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "substack.com",
    "beehiiv.com",
    "mail.beehiiv.com",
    "ghost.io",
    "buttondown.email",
    "convertkit.com",
    "mailchimp.com",
    "sendgrid.net",
}

PROMO_HOST_MARKERS = (
    "advertise.",
    "sponsors.",
    "jobs.ashbyhq.com",
    "boards.greenhouse.io",
    "jobs.lever.co",
    "workable.com",
    "ref.",
    "framer.link",
    "cloudonair.withgoogle.com",
    "login.tines.com",
    "magic.beehiiv.com",
)

PROMO_PATH_REGEX = re.compile(
    r"(?:^|/)(?:"
    r"subscribe|abonnement|abonnements|s-abonner|pricing|tarifs|plans|upgrade|membership|"
    r"signup|sign-up|register|checkout|gift|offrir|refer|referral|referrals|parrainage|leaderboard|"
    r"account|mon-compte|preferences|manage|advertise|advertising|sponsor|sponsors|sponsorship|media-kit|partenariats|"
    r"demo|book-a-demo|request-demo|free-trial|lp|landing|whitepapers|ebooks|webinars|"
    r"marketplace/build-learn|events/summit|resource-center|resources/ebooks|"
    r"jobs|careers|recrutement|emploi|offres-emploi|boutique|shop|store|merch|bons-plans|bon-plan"
    r")(?:/|$|\?)",
    re.IGNORECASE,
)

PROMO_QUERY_MARKERS = (
    "utm_medium=sponsor",
    "utm_campaign=sponsor",
    "utm_content=sponsor",
    "utm_medium=referral",
    "_sponsor",
    "primary-cta",
    "secondary-cta",
    "body_outro",
    "sfcid=",
    "mras=",
)

SELF_PROMO_AND_CTA_TITLE_RE = re.compile(
    r"(\(sponsor\)|\[sponsor\]|\bsponsored by\b|\btogether with\b|\bpresented by\b|"
    r"\bsponsorisé par\b|\ben partenariat avec\b|\bcontenu sponsorisé\b|"
    r"\bblack friday\b|\bcyber monday\b|\bfrench days\b|\bprime day\b|\bbon plan\b|\bbons plans\b|\bcode promo\b|\bà prix cassé\b|"
    r"\bf/h\b|\bh/f\b|"
    r"\babonnez-vous\b|\bs'abonner\b|\boffrir un abonnement\b|\boffre spéciale\b|\boffre abonnés\b|"
    r"\bgérer mes newsletters\b|\bdécouvrez nos newsletters\b|\bdécouvrez notre newsletter\b|"
    r"\bdécouvrez nos offres\b|\binscrivez-vous à\b|\brecevoir cette newsletter\b|"
    r"\btéléchargez l'application\b|\blire dans l'application\b|\bécoutez notre podcast\b|"
    r"\bdécouvrez notre podcast\b|\bnotre hors-série\b|\bparrainez\b|\binvitez vos amis\b|"
    r"\bsoutenez notre\b|\bsoutenir notre rédaction\b|\bfaire un don\b|"
    r"\bsubscribe to\b|\bupgrade to paid\b|\bbecome a paid subscriber\b|\bbecome a member\b|"
    r"\bgift a subscription\b|\bmanage your subscription\b|\bcheck out our other newsletters\b|"
    r"\bread in the app\b|\bdownload the app\b|\bget the app\b|\blisten to our podcast\b|"
    r"\brefer a friend\b|\bshare this newsletter\b|\bearn rewards\b|"
    r"\badvertise with us\b|\bsponsor this newsletter\b|\bwork with us\b|\bwe're hiring\b|\bwe are hiring\b|"
    r"\bat tldr\b|\$\d+k\s*base|\bstay subscribed\b|\bremain a subscriber\b|"
    r"^(?:download|téléchargez|télécharger)\s+(?:the|le|la|l'|notre|our|your|free)\s+(?:guide|report|whitepaper|ebook|playbook|rapport|livre blanc|étude)|"
    r"^(?:try|essayez)\s+.*\b(?:free|gratuit|gratuitement|today)\b|"
    r"^(?:get started|commencez|démarrez|launch your|lancez votre|save your virtual seat|join virtually|"
    r"book a demo|request a demo|schedule a demo|get your .* api key|get more agents|"
    r"read the blog post|explore the .* guides|see how .* works|see how .* build|"
    r"learn how to .* in minutes|add .* in under \d+ minutes|stop answering the same question)|"
    r"[>→↗]\s*$)",
    re.IGNORECASE,
)

SECTION_PROMO_CONTEXT_RE = re.compile(
    r"(\(sponsor\)|\[sponsor\]|\bsponsored by\b|\btogether with\b|\bpresented by\b|"
    r"\ba message from our sponsor\b|\bthank you to our sponsor\b|"
    r"\bsponsorisé par\b|\ben partenariat avec\b|\bpartenaire de cette édition\b|"
    r"\bwant to advertise in\b|\badvertise with us\b|\bsponsor this newsletter\b|"
    r"\brefer a friend\b|\bshare this newsletter\b|\bupgrade to paid\b|"
    r"\babonnez-vous à\b|\bdécouvrez nos autres newsletters\b|\boffre d'abonnement\b|"
    r"\bthe best stratechery content from the week\b|\bavailable on video\. you have three ways to consume it\b)",
    re.IGNORECASE,
)


def _extract_root_domain(host_or_email: str) -> str:
    """Extrait le domaine racine registrable simplifié (ex: 'sub.bytebytego.com' -> 'bytebytego.com')."""
    val = (host_or_email or "").strip().lower()
    if "@" in val:
        val = val.split("@", 1)[1]
    val = val.split(":", 1)[0].strip(".")
    if val.startswith("www."):
        val = val[4:]
    parts = [p for p in val.split(".") if p]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return val


def is_promotional_or_self_promo_link(
    raw_url: str | None,
    canonical_url: str | None,
    title: str | None,
    context_text: str = "",
    sender_email: str = "",
    source_title: str = "",
) -> tuple[bool, str]:
    """Détecte de manière déterministe si un lien extrait de newsletter ou de flux est :
    1. De l'autopromotion pour le service éditeur (abonnement, parrainage, compte, boutique, jobs, autres newsletters de l'éditeur).
    2. Un contenu sponsorisé, un appel à l'action commercial (CTA) ou une landing page marketing.
    Retourne `(is_promo, reason)`.
    """
    u_low = (raw_url or canonical_url or "").strip().lower()
    n_low = (canonical_url or raw_url or "").strip().lower()
    t_strip = " ".join((title or "").split())
    ctx_low = " ".join((context_text or "").split()).lower()
    src_combined = f"{sender_email} {source_title}".lower()

    parsed = urlparse(n_low if n_low.startswith("http") else u_low)
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    path = parsed.path.lower()

    # 1. Hôtes dédiés au sponsoring, recrutement ou redirections commerciales
    if any(h in host for h in PROMO_HOST_MARKERS):
        return True, f"promo_host:{host}"

    # 2. Autopromotion directe sur le domaine de l'éditeur (hors articles éditoriaux)
    link_root = _extract_root_domain(host)
    sender_root = _extract_root_domain(sender_email)
    if (
        sender_root
        and sender_root not in GENERIC_ESP_DOMAINS
        and link_root == sender_root
        and not host.startswith(("links.", "tracking.", "click."))
    ):
        if not EDITORIAL_ARTICLE_PATH_RE.search(path):
            return True, f"self_promo:sender_domain({link_root}{path[:24]})"

    if link_root in ("tldr.tech", "tldrnewsletter.com") and not host.startswith(("links.", "tracking.")):
        return True, f"self_promo:tldr_domain({host}{path[:24]})"

    # Autopromotion spécifique des plateformes d'édition (Substack, Beehiiv, Medium)
    if link_root == "substack.com" and not path.startswith("/p/") and not path.startswith("/home/post/"):
        return True, f"self_promo:substack_platform({path[:24]})"
    if link_root == "medium.com" and any(p in path for p in ("/membership", "/plans", "/gift", "/m/signin", "/about")):
        return True, f"self_promo:medium_platform({path[:24]})"

    # 3. Chemins d'URL typiques d'autopromotion éditeur ou de landing pages commerciales
    if PROMO_PATH_REGEX.search(path):
        return True, f"promo_path:{path[:40]}"

    # 4. Marqueurs de campagne sponsor / parrainage dans l'URL brute avant nettoyage UTM
    if any(q in u_low for q in PROMO_QUERY_MARKERS):
        return True, "promo_query:sponsor_or_referral_param"

    # 5. Titres d'autopromotion, de sponsoring ou de CTA marketing
    if SELF_PROMO_AND_CTA_TITLE_RE.search(t_strip):
        return True, "promo_title:cta_or_self_promo"

    # 6. Contexte parent immédiat signalant un bloc sponsorisé ou d'autopromotion
    if ctx_low and SECTION_PROMO_CONTEXT_RE.search(ctx_low):
        return True, "promo_context:sponsor_or_self_promo"

    # 7. Règle structurelle TLDR (uniquement lors de l'extraction initiale d'un email TLDR où sender_email est fourni) :
    # tout vrai sujet éditorial TLDR porte un suffixe de format `(X minute read)`, `(GitHub Repo)`, etc.
    if sender_email and "tldr" in sender_email.lower() and not TLDR_EDITORIAL_SUFFIX_RE.search(t_strip):
        return True, "tldr:missing_editorial_tag"

    # 8. Fragment de phrase en minuscule (lien inline au milieu d'un paragraphe publicitaire ou renvoi interne)
    if (
        t_strip
        and t_strip[0].islower()
        and not TLDR_EDITORIAL_SUFFIX_RE.search(t_strip)
    ):
        return True, "promo_title:lowercase_inline_fragment"

    return False, ""


GENERIC_BROADCAST_PLACEHOLDER_RE = re.compile(
    r"(?:,?\s*émission)?\s+du\s+(?:lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)\s+\d{1,2}\s+"
    r"(?:janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre)"
    r"\s+20\d{2}\s*$",
    re.IGNORECASE,
)


def is_generic_dated_broadcast_placeholder(title: str | None, excerpt: str | None = None) -> bool:
    """Détecte les entrées génériques de grille horaire radio/podcast sans sujet éditorial
    (ex: 'JOURNAL DE 09H00 du samedi 14 novembre 2026', 'Le 18/20 du vendredi 13 novembre 2026')
    qui n'ont pas de description spécifique.
    """
    t = " ".join((title or "").split())
    if not t or not GENERIC_BROADCAST_PLACEHOLDER_RE.search(t):
        return False
    clean_ex = clean_aggregator_noise(excerpt or "")
    if clean_ex.lower() == t.lower():
        clean_ex = ""
    return len(clean_ex) < 35


def clean_aggregator_noise(text_str: str) -> str:
    if not text_str:
        return ""
    s = re.sub(r"Article URL:\s*https?://\S+\s*", "", text_str)
    s = re.sub(r"Comments URL:\s*https?://\S+\s*", "", s)
    s = re.sub(r"Points:\s*\d+\s*#?\s*Comments:\s*\d+", "", s)
    s = re.sub(r"\(\d+\s*min(?:ute)?s?\s*read\)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\([^()]{1,40}·\s*\d+\s*min(?:ute)?s?\s*read\)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\(GitHub\s*Repo\)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\bMember-only story\b", "", s, flags=re.IGNORECASE)
    s = re.sub(
        r"\bHi everyone,\s*welcome back\.?\s*(?:Today,\s*I will be going over\s*[.…]*\s*)?",
        "",
        s,
        flags=re.IGNORECASE,
    )
    # Supprime les encarts autopromotionnels multi-lignes [Entretien ...](url) ou [Ajouter ... Google](url)
    s = re.sub(
        r"\[\s*(?:Entretien|À lire aussi|Lire aussi|Ajouter [^\]]+ à vos sources)[\s\S]*?\]\(https?://[^)]*\)",
        " ",
        s,
        flags=re.IGNORECASE,
    )
    # Nettoie les liens Markdown coupés en fin de chaîne ex: "...](https://www.pourlascience.fr/sr/..."
    s = re.sub(r"\]\(https?://\S*$", "", s)
    # Nettoie les métadonnées techniques d'en-tête Markdown (- **Source RSS** : ..., - **Lien** : ...)
    s = re.sub(
        r"-\s*\*\*(?:Source(?:\s+RSS|\s+croisée)?|Lien|URL|Édition\s+Newsletter|Newsletter\s+NYT|Article\s+principal|Catégorie|Publié)[^*]*\*\*\s*:\s*(?:https?://\S+|[^\n•]+?(?=\s+-\s+\*\*|\s*•|\n|$))",
        " ",
        s,
        flags=re.IGNORECASE,
    )
    # Supprime les titres Markdown '#' répétés en tête d'extrait ou derrière un tag [Source]
    s = re.sub(r"(^|\[\s*[^\]]+\s*\]\s*)#+\s*", r"\1", s)
    return " ".join(s.split())


def _extract_editorial_body_without_headers(markdown_body: str, title: str = "") -> str:
    """Extrait uniquement les paragraphes éditoriaux d'un corps Markdown en ignorant
    le titre `# ...` et les lignes de métadonnées `- **Source RSS** : ...`, `- **Lien** : ...`.
    """
    if not markdown_body:
        return ""
    kept_lines: list[str] = []
    norm_title = " ".join((title or "").split()).lower()
    for raw_line in markdown_body.splitlines():
        line = raw_line.strip()
        if not line or line == "---":
            continue
        if line.startswith("#"):
            continue
        if re.match(
            r"^-\s*\*\*(?:Source|Lien|URL|Édition|Newsletter|Article|Catégorie|Publié|Méthode|Paywall)[^*]*\*\*\s*:",
            line,
            flags=re.IGNORECASE,
        ):
            continue
        cleaned = clean_aggregator_noise(line)
        if not cleaned or cleaned.lower() == norm_title:
            continue
        kept_lines.append(cleaned)
    return " ".join(kept_lines).strip()


def extract_story_profile(title: str, summary: str) -> tuple[set[str], set[int]]:
    """Extrait d'un titre et d'un résumé :
    1. Les entités/sujets spécifiques du titre (`title_subjects`), avec résolution d'alias.
    2. Les classes d'événements détectées (`concepts` : ex. OUTAGE, SECURITY).
    """
    c_title = clean_aggregator_noise(title)
    c_sum = clean_aggregator_noise(summary)
    t_words = re.findall(r"[a-z0-9àâçéèêëîïôûùüÿñæœ]{3,}", c_title.lower())
    s_words = re.findall(r"[a-z0-9àâçéèêëîïôûùüÿñæœ]{3,}", c_sum.lower())
    all_raw = set(t_words) | set(s_words)

    title_subjects: set[str] = set()
    for w in t_words:
        if w not in GENERIC_OR_STOPWORDS and not w.isdigit():
            title_subjects.add(w)
            if w in SYNONYM_MAP:
                title_subjects.add(SYNONYM_MAP[w])

    for w in s_words:
        mapped = SYNONYM_MAP.get(w, w)
        if mapped == "microsoft" and {"panne", "outage", "down", "perturbation", "perturbations"} & all_raw:
            title_subjects.add("microsoft")
        elif mapped == "nvidia" and {"scandale", "scandal", "scandals", "chute", "fall", "exit", "resign"} & all_raw:
            title_subjects.add("nvidia")

    concepts = {idx for idx, grp in enumerate(EVENT_CONCEPT_GROUPS) if all_raw & grp}
    return title_subjects, concepts


def compute_hybrid_story_match(
    cos_sim: float,
    title_subj_a: set[str],
    concepts_a: set[int],
    title_subj_b: set[str],
    concepts_b: set[int],
) -> tuple[bool, float]:
    """Évalue si deux articles traitent du même événement d'actualité (Hybrid Story Clustering).
    Combine la similarité cosinus multilingue (`paraphrase-multilingual-MiniLM-L12-v2`) avec
    la concordance des entités nommées (`title_subj`) et du type d'événement (`concepts`).
    """
    shared_subj = title_subj_a & title_subj_b
    shared_concepts = concepts_a & concepts_b

    bonus = 0.0
    if shared_subj:
        bonus += min(0.16, 0.08 * len(shared_subj))
    if shared_concepts and shared_subj:
        bonus += 0.18
    hybrid_score = round(cos_sim + bonus, 4)

    # 1. Très forte proximité sémantique + au moins 1 entité spécifique commune
    if cos_sim >= 0.78 and len(shared_subj) >= 1:
        return True, hybrid_score
    # 2. Même événement caractérisé (ex: panne/outage, faille/breach) sur la même entité (y compris FR <-> EN)
    if cos_sim >= 0.40 and bool(shared_concepts) and len(shared_subj) >= 1:
        return True, hybrid_score
    # 3. Proximité sémantique modérée-haute + au moins 2 entités spécifiques communes dans le titre
    if cos_sim >= 0.62 and len(shared_subj) >= 2:
        return True, hybrid_score
    # 4. Quasi-identique sémantiquement (ex: même titre traduit ou reformulé) avec garde-fou sur les entités distinctes
    if cos_sim >= settings.topic_similarity_threshold and (len(shared_subj) >= 1 or not (title_subj_a and title_subj_b)):
        return True, hybrid_score

    return False, hybrid_score


def canonicalize_url(raw_url: str) -> str:
    raw = (raw_url or "").strip()
    if not raw or not raw.startswith(("http://", "https://")):
        return raw

    # Dépliage déterministe des trackers AWS SES / TLDR (/CL0/https:%2F%2F.../)
    cl0_match = re.search(r"/CL0/(https?(?:%3A|:)%2F%2F[^/]+)", raw, re.IGNORECASE)
    if cl0_match:
        raw = unquote(cl0_match.group(1))

    parsed = urlparse(raw)
    clean_query = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=False)
        if k.lower() not in TRACKING_PARAMS and not k.lower().startswith("utm_")
    ]
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/") or "/"
    return urlunparse(
        (
            parsed.scheme.lower(),
            netloc,
            path,
            "",
            urlencode(sorted(clean_query)),
            "",
        )
    )


def _average_vectors(v1: list[float], w1: int, v2: list[float], w2: int = 1) -> list[float]:
    arr = (np.asarray(v1, dtype=np.float32) * w1) + (np.asarray(v2, dtype=np.float32) * w2)
    norm = float(np.linalg.norm(arr)) or 1.0
    return (arr / norm).astype(float).tolist()


def _extract_url_slug_tokens(url: str | None) -> str:
    """Extrait les mots-clés lisibles du chemin/slug d'une URL éditoriale
    (ex: 'https://thenewstack.io/gemini-4-hydrogen-orchestrator' -> 'gemini 4 hydrogen orchestrator').
    Ignore les identifiants opaques (Google News RSS, hashs hexadécimaux, URNs).
    """
    if not url or url.startswith("urn:") or "news.google.com" in url:
        return ""
    try:
        parsed = urlparse(url)
        path = unquote(parsed.path or "")
    except Exception:
        return ""
    raw_tokens = re.split(r"[^a-zA-Z0-9]+", path.lower())
    clean_tokens: list[str] = []
    for tok in raw_tokens:
        if not tok or len(tok) > 32:
            continue
        if tok in {"html", "htm", "php", "aspx", "rss", "xml", "feed", "article", "articles", "blog", "post", "posts", "news", "story", "stories", "fr", "en"}:
            continue
        if tok.isdigit() and len(tok) > 4:
            continue
        if len(tok) >= 12 and all(c in "0123456789abcdef" for c in tok):
            continue
        clean_tokens.append(tok)
    return " ".join(clean_tokens[:25])


def ingest_content_item(
    session: Session,
    *,
    user_id: UUID,
    source: Source,
    raw_url: str,
    title: str,
    markdown_body: str,
    excerpt: str | None = None,
    author: str | None = None,
    published_at: datetime | None = None,
    external_id: str | None = None,
    curator_note: str | None = None,
    visibility: str = "private",
    category_slug: str = "tech",
) -> tuple[CanonicalItem, Topic, bool]:
    """Ingère un contenu (article RSS, sujet de newsletter Gmail, article abonné MCP),
    stocke son Markdown dans S3, calcule son embedding 384d et l'associe à un `topic_id` unique.
    Retourne `(item, topic, is_new_mention)`.
    """
    clean_title = " ".join((title or "Sans titre").split())[:500]
    clean_body = (markdown_body or clean_title).strip()
    editorial_fallback = _extract_editorial_body_without_headers(clean_body, clean_title)
    raw_excerpt = " ".join(((excerpt or "").strip() or editorial_fallback).split())[:600]
    clean_excerpt = clean_aggregator_noise(raw_excerpt)
    norm_url = canonicalize_url(raw_url) or f"urn:morning-review:{source.id}:{external_id or clean_title[:64]}"
    meta_dict = source.metadata_json or {}
    meta_creator = str(meta_dict.get("creator") or "").strip()
    meta_labels_str = " ".join(str(x) for x in (meta_dict.get("labels") or []))
    url_slug_text = " ".join(filter(None, [_extract_url_slug_tokens(norm_url), meta_creator, meta_labels_str])).strip()
    ext_id = (external_id or norm_url)[:500]
    pub_dt = published_at or utcnow()

    existing_mention = session.scalar(
        select(SourceMention).where(
            SourceMention.source_id == source.id,
            SourceMention.external_id == ext_id,
        )
    )
    if existing_mention is not None:
        item = session.get(CanonicalItem, existing_mention.item_id)
        topic = session.get(Topic, item.topic_id) if item and item.topic_id else None
        if item and topic:
            if published_at is not None and item.published_at != pub_dt:
                item.published_at = pub_dt
                from morning_review.pipeline.synthesizer import sync_topic_source_dates

                sync_topic_source_dates(session, topic)
            return item, topic, False

    owner_id = None if visibility == "public" else user_id
    content_hash = storage.compute_sha256(clean_body)
    s3_key = storage.build_item_key(content_hash, visibility=visibility, user_id=owner_id)
    storage.put_markdown(s3_key, clean_body)

    # 1. Recherche d'un CanonicalItem identique déjà présent dans le même périmètre de visibilité
    item_query = select(CanonicalItem).where(CanonicalItem.canonical_url == norm_url)
    if visibility == "public":
        item_query = item_query.where(CanonicalItem.visibility == "public")
    else:
        item_query = item_query.where(
            or_(
                CanonicalItem.visibility == "public",
                CanonicalItem.user_id == user_id,
            )
        )
    item = session.scalar(item_query)

    embed_text = f"{clean_aggregator_noise(clean_title)}\n{clean_excerpt}\n{url_slug_text}\n{curator_note or ''}".strip()
    vec = embedding_service.embed_one(embed_text)

    if item is None:
        item = CanonicalItem(
            visibility=visibility,
            user_id=owner_id,
            canonical_url=norm_url,
            title=clean_title,
            author=author[:250] if author else None,
            excerpt=clean_excerpt or raw_excerpt,
            category_slug=category_slug,
            content_sha256=content_hash,
            s3_key=s3_key,
            published_at=pub_dt,
            embedding=vec,
            embedding_model_version=embedding_service.model_version,
            fts=func.to_tsvector("simple", f"{clean_title} {clean_excerpt} {url_slug_text}".strip()),
        )
        session.add(item)
        session.flush()

    # 2. Attribution ou création du Topic unique (`topic_id`)
    topic: Topic | None = None
    matched_score: float = 1.0
    if item.topic_id:
        topic = session.get(Topic, item.topic_id)

    if topic is None:
        # 2a. Correspondance exacte d'URL primaire
        url_topic_stmt = select(Topic).where(
            Topic.primary_url == norm_url,
            or_(Topic.visibility == "public", Topic.user_id == user_id),
        )
        topic = session.scalar(url_topic_stmt)

    if topic is None:
        # 2b. Clustering Hybride (Cosine HNSW + Entités Spécifiques + Classes d'Événement)
        window_start = utcnow() - timedelta(days=settings.topic_window_days)
        distance_expr = Topic.centroid_embedding.cosine_distance(vec)
        candidates = session.execute(
            select(Topic, (1.0 - distance_expr).label("similarity"))
            .where(
                Topic.centroid_embedding.is_not(None),
                Topic.last_seen_at >= window_start,
                or_(Topic.visibility == "public", Topic.user_id == user_id),
            )
            .order_by(distance_expr.asc())
            .limit(15)
        ).all()

        item_subj, item_concepts = extract_story_profile(clean_title, f"{clean_excerpt or ''} {url_slug_text}".strip())
        best_topic: Topic | None = None
        best_hybrid: float = 0.0

        for cand_topic, raw_sim in candidates:
            cos_sim = float(raw_sim or 0.0)
            if cos_sim < 0.38:
                continue
            cand_slug = _extract_url_slug_tokens(cand_topic.primary_url)
            cand_subj, cand_concepts = extract_story_profile(
                cand_topic.title,
                f"{cand_topic.summary or ''} {cand_slug}".strip(),
            )
            matched, hyb_score = compute_hybrid_story_match(
                cos_sim,
                item_subj,
                item_concepts,
                cand_subj,
                cand_concepts,
            )
            if matched and hyb_score > best_hybrid:
                best_topic = cand_topic
                best_hybrid = hyb_score

        if best_topic is not None:
            topic = best_topic
            matched_score = best_hybrid

    if topic is None:
        is_existing_topic = False
        topic = Topic(
            visibility=visibility,
            user_id=owner_id,
            title=clean_title,
            summary=curator_note or clean_excerpt or clean_title,
            category_slug=category_slug,
            primary_url=norm_url,
            source_count=1,
            centroid_embedding=vec,
            fts=func.to_tsvector("simple", f"{clean_title} {clean_excerpt} {url_slug_text} {curator_note or ''}".strip()),
            first_seen_at=pub_dt,
            last_seen_at=pub_dt,
        )
        session.add(topic)
        session.flush()
    else:
        is_existing_topic = True
        old_vec = list(topic.centroid_embedding) if topic.centroid_embedding is not None else vec
        topic.centroid_embedding = _average_vectors(old_vec, max(1, topic.source_count), vec, 1)
        if topic.first_seen_at is None or pub_dt < topic.first_seen_at:
            topic.first_seen_at = pub_dt
        if topic.last_seen_at is None or pub_dt > topic.last_seen_at:
            topic.last_seen_at = pub_dt
        if curator_note and (not topic.summary or len(curator_note) > len(topic.summary)):
            topic.summary = curator_note

    item.topic_id = topic.id

    existing_link = session.scalar(
        select(TopicItem).where(
            TopicItem.topic_id == topic.id,
            TopicItem.item_id == item.id,
        )
    )
    is_new_topic_item = existing_link is None
    if is_new_topic_item:
        session.add(
            TopicItem(
                topic_id=topic.id,
                item_id=item.id,
                similarity_score=matched_score,
                source_role="curation" if curator_note else "primary",
            )
        )

    if existing_mention is None:
        mention = SourceMention(
            source_id=source.id,
            item_id=item.id,
            user_id=owner_id,
            external_id=ext_id,
            raw_url=raw_url[:1024] if raw_url else None,
            curator_note=curator_note,
        )
        session.add(mention)
        source.item_count = (source.item_count or 0) + 1
    else:
        existing_mention.item_id = item.id

    source.last_fetched_at = utcnow()

    # Recalcul du nombre de sources indépendantes et des dates min/max rattachées à ce topic_id
    session.flush()
    from morning_review.pipeline.synthesizer import (
        check_and_reactivate_topic_if_novel,
        get_deduplicated_topic_articles,
        sync_topic_source_dates,
        synthesize_topic_summary,
    )

    if is_existing_topic:
        check_and_reactivate_topic_if_novel(
            session,
            topic=topic,
            new_title=clean_title,
            new_text=f"{curator_note or ''}\n{clean_excerpt or editorial_fallback[:800]}".strip(),
            source_title=source.title,
        )

    sync_topic_source_dates(session, topic)
    dedup_articles = get_deduplicated_topic_articles(session, topic.id)
    topic.source_count = max(1, len(dedup_articles))

    # Enrichissement profond automatique dès l'ingestion pour les articles de digests tronqués (ex: Medium Daily Digest)
    import os
    if not os.environ.get("PYTEST_CURRENT_TEST") and norm_url.startswith(("http://", "https://")):
        from morning_review.connectors.mcp_enricher import _detect_mcp_tool, enrich_topic_content

        mcp_tool, _ = _detect_mcp_tool(norm_url)
        if mcp_tool and len(clean_excerpt) < 280:
            try:
                enrich_topic_content(session, user_id=user_id, topic_id=topic.id)
            except Exception:
                pass

    synthesize_topic_summary(session, topic)

    return item, topic, True


def recluster_existing_topics(session: Session, user_id: UUID) -> dict[str, Any]:
    """Consolide rétroactivement les Topics existants en base selon l'algorithme de
    Hybrid Story Clustering et nettoie les résumés pollués par le boilerplate hnrss.org.
    """
    # 1. Suppression des anciens topics de fallback email complet `[TLDR...]` obsolètes
    legacy_tldr_topics = session.scalars(
        select(Topic).where(Topic.title.like("[TLDR%"))
    ).all()
    removed_legacy = len(legacy_tldr_topics)
    for lt in legacy_tldr_topics:
        for it in session.scalars(select(CanonicalItem).where(CanonicalItem.topic_id == lt.id)).all():
            session.delete(it)
        session.delete(lt)
    session.flush()

    # 1b. Dépliage rétroactif des URLs tracking.tldrnewsletter.com/CL0/ existantes
    for ci in session.scalars(
        select(CanonicalItem).where(CanonicalItem.canonical_url.like("%tracking.tldrnewsletter.com/CL0/%"))
    ).all():
        ci.canonical_url = canonicalize_url(ci.canonical_url)
    for tp in session.scalars(
        select(Topic).where(Topic.primary_url.like("%tracking.tldrnewsletter.com/CL0/%"))
    ).all():
        tp.primary_url = canonicalize_url(tp.primary_url or "")
    session.flush()

    # 1c. Purge rétroactive des sujets d'autopromotion éditeur et des CTAs sponsorisés
    promo_topic_ids: set[UUID] = set()
    rows_for_promo_audit = session.execute(
        select(Topic.id, Topic.title, CanonicalItem.canonical_url, SourceMention.raw_url, Source.title)
        .join(CanonicalItem, CanonicalItem.topic_id == Topic.id)
        .join(SourceMention, SourceMention.item_id == CanonicalItem.id)
        .join(Source, Source.id == SourceMention.source_id)
    ).all()
    for tid, t_title, c_url, r_url, s_title in rows_for_promo_audit:
        is_promo, _ = is_promotional_or_self_promo_link(
            raw_url=r_url,
            canonical_url=c_url,
            title=t_title,
            source_title=s_title or "",
        )
        if is_promo:
            promo_topic_ids.add(tid)

    removed_promo = len(promo_topic_ids)
    for ptid in promo_topic_ids:
        pt = session.get(Topic, ptid)
        if pt is not None:
            for it in session.scalars(select(CanonicalItem).where(CanonicalItem.topic_id == pt.id)).all():
                session.delete(it)
            session.delete(pt)
    session.flush()

    # 1d. Purge des grilles horaires génériques sans description ("JOURNAL DE 09H00 du samedi...")
    for ci in session.scalars(select(CanonicalItem)).all():
        if is_generic_dated_broadcast_placeholder(ci.title, ci.excerpt):
            session.delete(ci)
        elif ci.excerpt and ("Source RSS" in ci.excerpt or "Source croisée" in ci.excerpt or ci.excerpt.startswith("# ")):
            cleaned_ex = clean_aggregator_noise(ci.excerpt)
            if cleaned_ex.lower() == (ci.title or "").lower():
                cleaned_ex = ""
            ci.excerpt = cleaned_ex
    session.flush()

    for orphan_tp in session.scalars(select(Topic)).all():
        rem_items = session.scalars(select(CanonicalItem).where(CanonicalItem.topic_id == orphan_tp.id)).all()
        if not rem_items:
            session.delete(orphan_tp)
    session.flush()

    # 2. Nettoyage des résumés hnrss.org / Source RSS et recalcul de leur embedding
    noisy_topics = session.scalars(
        select(Topic).where(
            or_(
                Topic.summary.like("%Comments URL: https://news.ycombinator.com%"),
                Topic.summary.ilike("%Source RSS%"),
                Topic.summary.ilike("%Source croisée%"),
                Topic.summary.like("# %"),
            )
        )
    ).all()
    for nt in noisy_topics:
        cleaned = clean_aggregator_noise(nt.summary or "")
        nt.summary = cleaned or nt.title
        nt.centroid_embedding = embedding_service.embed_one(
            f"{clean_aggregator_noise(nt.title)}\n{cleaned}".strip()
        )
    session.flush()

    # 3. Regroupement hybride des Topics actifs dans la fenêtre temporelle
    topics = session.scalars(
        select(Topic)
        .where(
            Topic.centroid_embedding.is_not(None),
            or_(Topic.visibility == "public", Topic.user_id == user_id),
        )
        .order_by(Topic.source_count.desc(), Topic.first_seen_at.asc())
    ).all()

    profile_cache: dict[UUID, tuple[np.ndarray, set[str], set[int]]] = {}
    for t in topics:
        vec = np.asarray(list(t.centroid_embedding), dtype=np.float32)
        norm = float(np.linalg.norm(vec)) or 1.0
        slug_toks = _extract_url_slug_tokens(t.primary_url)
        subj, concepts = extract_story_profile(t.title, f"{t.summary or ''} {slug_toks}".strip())
        profile_cache[t.id] = (vec / norm, subj, concepts)

    merged_into: dict[UUID, UUID] = {}
    merges_log: list[dict[str, Any]] = []

    for i, target in enumerate(topics):
        if target.id in merged_into:
            continue
        v_target, subj_target, conc_target = profile_cache[target.id]

        for j in range(i + 1, len(topics)):
            cand = topics[j]
            if cand.id in merged_into:
                continue
            delta_days = abs((target.last_seen_at - cand.last_seen_at).total_seconds()) / 86400.0
            if delta_days > settings.topic_window_days:
                continue

            v_cand, subj_cand, conc_cand = profile_cache[cand.id]
            cos_sim = float(np.dot(v_target, v_cand))
            if cos_sim < 0.38:
                continue

            matched, hyb_score = compute_hybrid_story_match(
                cos_sim,
                subj_target,
                conc_target,
                subj_cand,
                conc_cand,
            )
            if not matched:
                continue

            # Fusionne `cand` dans `target`
            merged_into[cand.id] = target.id
            subj_target |= subj_cand
            conc_target |= conc_cand

            cand_items = session.scalars(
                select(CanonicalItem).where(CanonicalItem.topic_id == cand.id)
            ).all()
            for ci in cand_items:
                ci.topic_id = target.id
                existing_ti = session.scalar(
                    select(TopicItem).where(
                        TopicItem.topic_id == target.id,
                        TopicItem.item_id == ci.id,
                    )
                )
                if existing_ti is None:
                    session.add(
                        TopicItem(
                            topic_id=target.id,
                            item_id=ci.id,
                            similarity_score=hyb_score,
                            source_role="clustered",
                        )
                    )

            # Transfère d'éventuels votes utilisateur de cand vers target
            for fb in session.scalars(
                select(UserTopicFeedback).where(UserTopicFeedback.topic_id == cand.id)
            ).all():
                existing_fb = session.scalar(
                    select(UserTopicFeedback).where(
                        UserTopicFeedback.user_id == fb.user_id,
                        UserTopicFeedback.topic_id == target.id,
                    )
                )
                if existing_fb is None:
                    fb.topic_id = target.id
                else:
                    session.delete(fb)

            # Transfère / réconcilie l'état d'archivage (UserTopicState) et vérifie si `cand` apporte de la nouveauté
            from morning_review.pipeline.synthesizer import check_and_reactivate_topic_if_novel

            for st in session.scalars(
                select(UserTopicState).where(UserTopicState.topic_id == cand.id)
            ).all():
                existing_st = session.scalar(
                    select(UserTopicState).where(
                        UserTopicState.user_id == st.user_id,
                        UserTopicState.topic_id == target.id,
                    )
                )
                if existing_st is None:
                    st.topic_id = target.id
                else:
                    session.delete(st)
            session.flush()
            check_and_reactivate_topic_if_novel(
                session,
                topic=target,
                new_title=cand.title,
                new_text=cand.summary or cand.title,
                source_title="Nouvelle source consolidée",
            )

            # Choisit le résumé le plus informatif
            if cand.summary and len(cand.summary) > len(target.summary or ""):
                target.summary = cand.summary
            if cand.last_seen_at > target.last_seen_at:
                target.last_seen_at = cand.last_seen_at

            merges_log.append(
                {
                    "kept_topic_id": str(target.id),
                    "kept_title": target.title,
                    "merged_topic_id": str(cand.id),
                    "merged_title": cand.title,
                    "cosine_similarity": round(cos_sim, 4),
                    "hybrid_score": hyb_score,
                }
            )
            session.delete(cand)

    session.flush()

    # 4. Recalcule la synthèse uniquement sur les Topics effectivement fusionnés (ou sans résumé)
    from morning_review.pipeline.synthesizer import synthesize_topic_summary

    kept_ids = {UUID(m["kept_topic_id"]) for m in merges_log}
    remaining_topics = session.scalars(select(Topic)).all()
    for rt in remaining_topics:
        if rt.id in kept_ids or not (rt.summary or "").strip():
            synthesize_topic_summary(session, rt)

    session.commit()
    return {
        "legacy_WholeEmail_removed": removed_legacy,
        "promotional_topics_removed": removed_promo,
        "topics_merged_count": len(merges_log),
        "remaining_topics_count": len(remaining_topics),
        "merges": merges_log,
    }
