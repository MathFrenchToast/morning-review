from __future__ import annotations

from email.message import EmailMessage
from uuid import UUID, uuid4

from morning_review.connectors.gmail_imap import (
    extract_curated_links_from_html,
    is_newsletter_message,
)
from morning_review.connectors.source_registry import (
    list_user_sources,
    register_discovered_newsletter,
    sync_sources_from_yaml,
)
from morning_review.db.models import CanonicalItem, Source, Topic, User, UserProfile
from morning_review.db.session import SessionLocal, init_db
from morning_review.pipeline.ingestion import ingest_content_item
from morning_review.pipeline.search_and_profile import (
    generate_daily_digest,
    record_topic_feedback,
    search_topics,
)


def test_uuid_user_and_yaml_sync() -> None:
    user_id = init_db()
    assert isinstance(user_id, UUID)

    with SessionLocal() as session:
        sync_res = sync_sources_from_yaml(session, user_id=user_id)
        assert sync_res["categories_synced"] >= 4
        assert sync_res["sources_synced"] >= 3

        sources = list_user_sources(session, user_id=user_id)
        assert len(sources) >= 3


def test_newsletter_detection_and_link_extraction() -> None:
    nl_msg = EmailMessage()
    nl_msg["From"] = "TLDR Tech <dan@tldrnewsletter.com>"
    nl_msg["Subject"] = "Kubernetes 1.32 & eBPF Service Mesh"
    nl_msg["List-Unsubscribe"] = "<https://tldr.tech/unsubscribe>"
    nl_msg["List-Id"] = "<tech.tldrnewsletter.com>"
    html = """
    <html><body>
      <p>Here is today's curation on cloud native architecture and eBPF networking:</p>
      <p>
        <a href="https://kubernetes.io/blog/2026/11/ebpf-dataplane/?utm_source=tldr&utm_medium=email">
          Kubernetes eBPF Dataplane Deep Dive and Benchmarks (8 minute read)
        </a>
        A detailed benchmark of Cilium eBPF dataplane latency in multi-tenant clusters.
      </p>
      <div class="sponsor-block">
        <h3><a href="https://acme-cloud.io/?utm_medium=sponsor">Acme Cloud Native (Sponsor)</a></h3>
        <p>
          Accelerate your cluster deployments today and
          <a href="https://acme-cloud.io/resources/ebooks/k8s-guide">Download the guide</a>
          or <a href="https://ref.acme-cloud.io/tldr-primary-cta">Try Acme Cloud free</a>.
        </p>
      </div>
      <p>
        <a href="https://jobs.ashbyhq.com/tldr.tech/7aefaa32">
          Senior Software Engineer, Applied AI at TLDR ($300k base)
        </a>
      </p>
      <p><a href="https://tldr.tech/unsubscribe">Unsubscribe</a></p>
    </body></html>
    """
    is_nl, reason = is_newsletter_message(nl_msg, html_body=html)
    assert is_nl is True
    assert "List-Id" in reason

    links = extract_curated_links_from_html(html, "TLDR Tech", sender_email="dan@tldrnewsletter.com")
    assert len(links) == 1
    assert links[0]["canonical_url"] == "https://kubernetes.io/blog/2026/11/ebpf-dataplane"

    # Vérifie le filtrage de l'autopromotion éditeur (domaine expéditeur, Substack, FR/EN)
    from morning_review.connectors.gmail_imap import is_self_promotional_email
    from morning_review.pipeline.ingestion import is_promotional_or_self_promo_link

    # 1. Lien vers le propre service/cours de l'éditeur (autopromotion même domaine)
    is_p1, r1 = is_promotional_or_self_promo_link(
        raw_url="https://bytebytego.com/courses/system-design-interview",
        canonical_url="https://bytebytego.com/courses/system-design-interview",
        title="Master the System Design Interview with ByteByteGo",
        sender_email="alex@bytebytego.com",
        source_title="ByteByteGo Newsletter",
    )
    assert is_p1 is True and "self_promo:sender_domain" in r1

    # 2. Autopromotion en français (abonnement / autres newsletters / application)
    is_p2, _ = is_promotional_or_self_promo_link(
        raw_url="https://www.lemonde.fr/abonnements/",
        canonical_url="https://lemonde.fr/abonnements",
        title="Abonnez-vous au Monde dès 1 € le premier mois",
        sender_email="newsletters@lemonde.fr",
        source_title="Le Monde",
    )
    assert is_p2 is True

    # 3. Email entier d'autopromotion éditeur (offre spéciale / bienvenue / parrainage)
    is_mail_promo, _ = is_self_promotional_email("Offre spéciale : abonnez-vous à -50% pour le Black Friday")
    assert is_mail_promo is True

    # Vérifie le dépliage des liens de tracking AWS SES /CL0/https:%2F%2F...
    from morning_review.pipeline.ingestion import canonicalize_url

    ses_url = (
        "https://tracking.tldrnewsletter.com/CL0/"
        "https:%2F%2Fwww.wiz.io%2Fblog%2Fgheist-github%3Futm_source=tldrdevops/1/010001a1/sig=452"
    )
    assert canonicalize_url(ses_url) == "https://wiz.io/blog/gheist-github"

    # Vérifie l'exclusion des emails transactionnels OTP / sécurité
    otp_msg = EmailMessage()
    otp_msg["From"] = "no-reply@accounts.google.com"
    otp_msg["Subject"] = "Your verification code is 482910"
    is_otp_nl, _ = is_newsletter_message(otp_msg, text_body="Your verification code")
    assert is_otp_nl is False


def test_multisource_topic_deduplication_search_and_profiling() -> None:
    user_id = init_db()
    other_user_id = uuid4()

    with SessionLocal() as session:
        if session.get(User, other_user_id) is None:
            session.add(User(id=other_user_id, email=f"user-{other_user_id}@example.com"))
            session.add(UserProfile(user_id=other_user_id))
            session.commit()

        nl_src, _ = register_discovered_newsletter(
            session,
            user_id=user_id,
            sender_name="ByteByteGo Newsletter",
            sender_email="alex@bytebytego.com",
            list_id="bytebytego.substack.com",
            subject_hint="PostgreSQL pgvector HNSW architecture",
        )
        rss_src = session.query(Source).filter(Source.kind == "rss").first()
        assert rss_src is not None

        # Nettoyage préventif si un run précédent a laissé l'item de test
        for old_ci in session.query(CanonicalItem).filter(
            CanonicalItem.canonical_url.like("%pgvector-hybrid-search%")
        ).all():
            session.delete(old_ci)
        for old_tp in session.query(Topic).filter(
            Topic.primary_url.like("%pgvector-hybrid-search%")
        ).all():
            session.delete(old_tp)
        session.commit()

        from datetime import datetime, timezone

        t_first = datetime(2026, 11, 10, 8, 30, tzinfo=timezone.utc)
        t_latest = datetime(2026, 11, 13, 14, 45, tzinfo=timezone.utc)

        shared_url = "https://engineering.example.com/posts/pgvector-hybrid-search"
        item_1, topic_1, _ = ingest_content_item(
            session,
            user_id=user_id,
            source=rss_src,
            raw_url=f"{shared_url}?utm_source=rss",
            title="Hybrid Search with PostgreSQL pgvector HNSW and GIN tsvector",
            markdown_body="Combining HNSW halfvec and GIN tsvector with Reciprocal Rank Fusion.",
            published_at=t_first,
            visibility="public",
            category_slug="tech",
        )
        item_2, topic_2, _ = ingest_content_item(
            session,
            user_id=user_id,
            source=nl_src,
            raw_url="https://bytebytego.com/p/pgvector-hybrid-search-rrf",
            title="Hybrid Search with PostgreSQL pgvector HNSW and GIN tsvector",
            markdown_body="Curated note on combining HNSW halfvec and GIN tsvector in Postgres.",
            curator_note="Great breakdown of RRF in PostgreSQL.",
            published_at=t_latest,
            visibility="private",
            category_slug="tech",
        )
        session.commit()

        # Les deux sources indépendantes (RSS + ByteByteGo Newsletter) pointent vers le même topic_id unique (2 sources) !
        assert topic_1.id == topic_2.id
        assert topic_2.source_count >= 2
        assert topic_2.first_seen_at == t_first
        assert topic_2.last_seen_at == t_latest

        # Vérifie que deux déclinaisons TLDR (TLDR + TLDR Dev) pour un même article ne comptent que pour 1 seule source !
        tldr_main, _ = register_discovered_newsletter(
            session,
            user_id=user_id,
            sender_name="TLDR",
            sender_email="dan@tldrnewsletter.com",
            list_id="tldr.tech",
            subject_hint="TLDR Daily",
        )
        tldr_dev, _ = register_discovered_newsletter(
            session,
            user_id=user_id,
            sender_name="TLDR Dev",
            sender_email="dan@tldrnewsletter.com",
            list_id="tldr-dev.tech",
            subject_hint="TLDR Dev",
        )
        tldr_item_a, tldr_topic, _ = ingest_content_item(
            session,
            user_id=user_id,
            source=tldr_main,
            raw_url="https://claude.dev/blog/code-review-test-fixture",
            title="How the Claude Code Team Does Code Review (18 minute read)",
            markdown_body="Anthropic shifted to Claude Code for PR reviews.",
            visibility="private",
            category_slug="tech",
        )
        tldr_item_b, tldr_topic_b, _ = ingest_content_item(
            session,
            user_id=user_id,
            source=tldr_dev,
            raw_url="https://links.tldrnewsletter.com/code-review-test-fixture",
            title="How the Claude Code Team Does Code Review (16 minute read)",
            markdown_body="Anthropic shifted to Claude Code for PR reviews.",
            visibility="private",
            category_slug="tech",
        )
        session.commit()
        assert tldr_topic.id == tldr_topic_b.id
        assert tldr_topic_b.source_count == 1
        session.delete(tldr_item_a)
        if tldr_item_b.id != tldr_item_a.id:
            session.delete(tldr_item_b)
        session.delete(tldr_topic)
        session.commit()

        # Recherche hybride avec filtre et tri sur la date de première / dernière source
        hits = search_topics(
            session,
            user_id=user_id,
            query="pgvector HNSW Reciprocal Rank",
            mode="hybrid",
            date_field="first_seen_at",
            date_from="2026-11-10",
            sort_by="last_seen_at",
            sort_order="desc",
        )
        matched_hit = next((h for h in hits if h["topic_id"] == str(topic_1.id)), None)
        assert matched_hit is not None
        assert matched_hit["first_seen_at"] == t_first.isoformat()
        assert matched_hit["last_seen_at"] == t_latest.isoformat()

        # Auto-profilage (+1 pertinent)
        fb_res = record_topic_feedback(session, user_id=user_id, topic_id=topic_1.id, vote=1)
        assert fb_res["positive_votes"] >= 1
        assert fb_res["category_affinities"]["tech"] > 0

        # Génération du rapport quotidien Morning Review
        digest = generate_daily_digest(session, user_id=user_id)
        assert digest["topics_count"] >= 1

        # Vérifie le routage vers agent-auth-mcp pour les sources abonnées
        from morning_review.connectors.mcp_enricher import _detect_mcp_tool

        assert _detect_mcp_tool("https://www.newscientist.com/article/123")[0] == "newscientist"
        assert _detect_mcp_tool("https://www.pourlascience.fr/sd/physique/test.php")[0] == "pourlascience"
        assert _detect_mcp_tool("https://www.quantamagazine.org/some-article/")[0] is None

        # Nettoyage des fixtures de test
        session.delete(nl_src)
        session.delete(item_1)
        if item_2.id != item_1.id:
            session.delete(item_2)
        session.delete(topic_1)
        other_u = session.get(User, other_user_id)
        if other_u:
            session.delete(other_u)
        session.commit()


def test_creators_aggregation_and_multilabel_filtering() -> None:
    from morning_review.connectors.source_registry import infer_source_labels_and_creator
    from morning_review.connectors.youtube_and_podcasts import list_creators_aggregated

    user_id = init_db()
    labels_pod, creator_pod = infer_source_labels_and_creator(
        kind="podcast",
        title="Underscore_ (Micode)",
        url="https://feeds.acast.com/public/shows/underscore",
        category_slug="tech",
    )
    labels_yt, creator_yt = infer_source_labels_and_creator(
        kind="youtube",
        title="Micode (YouTube)",
        url="https://www.youtube.com/feeds/videos.xml?channel_id=UCYnvxJ-PKiGXo_tYXpWAC-w",
        category_slug="tech",
    )
    assert creator_pod == "Micode / Underscore_"
    assert creator_yt == "Micode / Underscore_"
    assert "podcast" in labels_pod and "tech" in labels_pod
    assert "youtube" in labels_yt and "tech" in labels_yt

    with SessionLocal() as session:
        creators = list_creators_aggregated(session, user_id=user_id, only_creator_media=True)
        assert len(creators) >= 10
        micode_entry = next((c for c in creators if c["creator"] == "Micode / Underscore_"), None)
        assert micode_entry is not None
        assert micode_entry["channels_count"] >= 2
        assert "podcast" in micode_entry["labels"]
        assert "youtube" in micode_entry["labels"]

        # Vérifie le filtrage combiné multi-catégories/labels dans search_topics
        hits = search_topics(
            session,
            user_id=user_id,
            query="AI",
            mode="hybrid",
            category_slug="tech,podcast",
        )
        assert isinstance(hits, list)


def test_mark_as_read_archive_search_and_novelty_reactivation() -> None:
    from morning_review.db.models import UserTopicState
    from morning_review.pipeline.synthesizer import set_topic_archive_state

    user_id = init_db()
    with SessionLocal() as session:
        # Nettoyage préventif
        for old_ci in session.query(CanonicalItem).filter(
            CanonicalItem.canonical_url.like("%quantum-fault-tolerant-qpu-fixture%")
        ).all():
            session.delete(old_ci)
        for old_tp in session.query(Topic).filter(
            Topic.primary_url.like("%quantum-fault-tolerant-qpu-fixture%")
        ).all():
            session.delete(old_tp)
        session.commit()

        rss_src = session.query(Source).filter(Source.kind == "rss").first()
        assert rss_src is not None

        # 1. Ingestion initiale d'un sujet d'actualité scientifique
        item_1, topic, _ = ingest_content_item(
            session,
            user_id=user_id,
            source=rss_src,
            raw_url="https://example.org/news/quantum-fault-tolerant-qpu-fixture-1",
            title="IBM Unveils Heron R3 Quantum Processor with Logical Qubit Error Correction",
            markdown_body="IBM announced the Heron R3 quantum processor achieving logical qubit error correction in laboratory benchmarks.",
            excerpt="IBM announced the Heron R3 quantum processor achieving logical qubit error correction in laboratory benchmarks.",
            visibility="public",
            category_slug="science",
        )
        session.commit()

        # 2. Marquer comme lu / Archiver (Inbox Zero)
        arch_res = set_topic_archive_state(session, user_id=user_id, topic_id=topic.id, archived=True)
        assert arch_res["is_archived"] is True
        assert arch_res["has_updates"] is False

        # Le sujet ne doit plus apparaître dans la recherche Inbox (archived_mode="exclude")
        inbox_hits = search_topics(
            session,
            user_id=user_id,
            query="Heron R3 Quantum Processor",
            mode="keyword",
            archived_mode="exclude",
        )
        assert all(h["topic_id"] != str(topic.id) for h in inbox_hits)

        # Le sujet DOIT apparaître dans la recherche par mot-clé de l'onglet Archives (archived_mode="only")
        archive_hits = search_topics(
            session,
            user_id=user_id,
            query="Heron R3 Quantum",
            mode="keyword",
            archived_mode="only",
        )
        assert any(h["topic_id"] == str(topic.id) and h["is_archived"] is True for h in archive_hits)

        # 3. Un second article qui répète exactement la même information ne doit PAS réactiver le sujet
        item_2, topic_after_repeat, _ = ingest_content_item(
            session,
            user_id=user_id,
            source=rss_src,
            raw_url="https://example.org/news/quantum-fault-tolerant-qpu-fixture-1",
            title="IBM Unveils Heron R3 Quantum Processor with Logical Qubit Error Correction",
            markdown_body="IBM announced the Heron R3 quantum processor achieving logical qubit error correction in laboratory benchmarks.",
            excerpt="IBM announced the Heron R3 quantum processor achieving logical qubit error correction in laboratory benchmarks.",
            external_id="repeat-mention-fixture-2",
            visibility="public",
            category_slug="science",
        )
        session.commit()
        state_after_repeat = session.get(UserTopicState, {"user_id": user_id, "topic_id": topic.id})
        assert state_after_repeat is not None
        assert state_after_repeat.is_archived is True
        assert state_after_repeat.has_updates is False

        # 4. Un nouvel article apportant des faits et chiffres inédits DOIT réactiver le sujet avec mention des nouveautés
        item_3, topic_after_novelty, _ = ingest_content_item(
            session,
            user_id=user_id,
            source=rss_src,
            raw_url="https://example.org/news/quantum-fault-tolerant-qpu-fixture-1",
            title="IBM Heron R3 Quantum Processor Deployed at RIKEN Supercomputing Center in Japan",
            markdown_body=(
                "RIKEN in Japan signed a 140 million dollar contract to integrate the Heron R3 quantum "
                "processor directly with the Fugaku supercomputer by December 2026, reducing gate latency by 65%."
            ),
            excerpt=(
                "RIKEN in Japan signed a 140 million dollar contract to integrate the Heron R3 quantum "
                "processor directly with the Fugaku supercomputer by December 2026, reducing gate latency by 65%."
            ),
            curator_note=(
                "RIKEN in Japan signed a 140 million dollar contract to integrate the Heron R3 quantum "
                "processor directly with the Fugaku supercomputer by December 2026, reducing gate latency by 65%."
            ),
            external_id="novel-mention-fixture-3",
            visibility="public",
            category_slug="science",
        )
        session.commit()

        state_after_novelty = session.get(UserTopicState, {"user_id": user_id, "topic_id": topic.id})
        assert state_after_novelty is not None
        assert state_after_novelty.is_archived is False
        assert state_after_novelty.has_updates is True
        assert state_after_novelty.new_articles_count >= 1
        assert state_after_novelty.updates_summary is not None
        assert "Fugaku" in state_after_novelty.updates_summary or "RIKEN" in state_after_novelty.updates_summary

        # Le sujet réactivé réapparaît dans le flux principal avec has_updates=True !
        reactivated_hits = search_topics(
            session,
            user_id=user_id,
            query="Heron R3 Quantum",
            mode="keyword",
            archived_mode="exclude",
        )
        matched = next((h for h in reactivated_hits if h["topic_id"] == str(topic.id)), None)
        assert matched is not None
        assert matched["has_updates"] is True
        assert matched["is_archived"] is False

        # Nettoyage
        session.delete(state_after_novelty)
        for it in {item_1, item_2, item_3}:
            session.delete(it)
        session.delete(topic)
        session.commit()
