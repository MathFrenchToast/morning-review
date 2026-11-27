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
    assert canonicalize_url(ses_url) == "https://www.wiz.io/blog/gheist-github"
    assert (
        canonicalize_url("https://www.construction-physics.com/p/reading-list-11142026?utm_source=substack")
        == "https://www.construction-physics.com/p/reading-list-11142026"
    )

    # Vérifie l'exclusion des emails transactionnels OTP / sécurité
    otp_msg = EmailMessage()
    otp_msg["From"] = "no-reply@accounts.google.com"
    otp_msg["Subject"] = "Your verification code is 482910"
    is_otp_nl, _ = is_newsletter_message(otp_msg, text_body="Your verification code")
    assert is_otp_nl is False

    # Vérifie la détection et l'extraction d'un Medium Daily Digest transféré (Fwd:)
    from morning_review.connectors.gmail_imap import unwrap_forwarded_email_metadata

    fwd_msg = EmailMessage()
    fwd_msg["From"] = "Mathieu V <mathieu.veron@gmail.com>"
    fwd_msg["Subject"] = "Fwd: Helm in Argo CD Isn’t Helm | Tom Jose | DevOps in kotaicode"
    fwd_txt = (
        "---------- Forwarded message ---------\r\n"
        "De : Medium Daily Digest <noreply@medium.com>\r\n"
        "Date: ven. 13 nov. 2026 à 06:20\r\n"
        "Subject: Helm in Argo CD Isn’t Helm | Tom Jose | DevOps in kotaicode\r\n"
        "To: <mathieu.veron@gmail.com>\r\n"
    )
    fwd_html = """
    <html><body>
      <div>
        <a href="https://medium.com/@hassanzadeh.sd?source=email-abc-1794443294408-digest.reader">Sajjad Hassanzadeh</a>
        <a href="https://medium.com/@hassanzadeh.sd/cilium-is-more-than-a-cni-why-we-dont-need-kube-proxy-anymore-c996a7b51221?source=email-abc-1794443294408-digest.reader">
          <h2>Cilium Is More Than a CNI: Why We Don’t Need kube-proxy…</h2>
          <p>When I first started learning about Cilium, I thought it was just another CNI.</p>
        </a>
        <a href="https://medium.com/@dailynewsletter/write-by-any-means-necessary-8f3143b9f794?source=email-abc-1794443294408-digest.reader">
          <h2>Write by any means necessary</h2>
        </a>
      </div>
    </body></html>
    """
    unwrapped = unwrap_forwarded_email_metadata(fwd_msg, fwd_html, fwd_txt)
    assert unwrapped["sender_email"] == "noreply@medium.com"
    assert unwrapped["sender_name"] == "Medium Daily Digest"
    is_med_nl, med_reason = is_newsletter_message(fwd_msg, html_body=fwd_html, text_body=fwd_txt)
    assert is_med_nl is True
    assert "forwarded_sender" in med_reason

    med_links = extract_curated_links_from_html(
        fwd_html,
        unwrapped["sender_name"],
        sender_email=unwrapped["sender_email"],
        email_subject=unwrapped["subject"],
    )
    assert len(med_links) == 1
    assert "anymore" in med_links[0]["title"].lower()
    assert "When I first started learning about Cilium" in med_links[0]["curator_note"]


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

        from datetime import datetime, timedelta, timezone

        now_utc = datetime.now(timezone.utc).replace(microsecond=0)
        t_first = now_utc - timedelta(hours=36)
        t_latest = now_utc - timedelta(hours=2)

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
            date_from=t_first.date().isoformat(),
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
        session.flush()
        for it in {item_1, item_2, item_3}:
            session.delete(it)
        session.delete(topic)
        session.commit()


def test_vote_neutral_unvote_and_deep_detail_view() -> None:
    from morning_review.pipeline.synthesizer import build_topic_deep_detail

    user_id = init_db()
    with SessionLocal() as session:
        rss_src = session.query(Source).filter(Source.kind == "rss").first()
        assert rss_src is not None

        item, topic, _ = ingest_content_item(
            session,
            user_id=user_id,
            source=rss_src,
            raw_url="https://example.org/news/deep-detail-zero-click-fixture",
            title="Linux Kernel 6.19 Introduces EEVDF Scheduler Optimizations for Low-Latency Workloads",
            markdown_body=(
                "# Linux Kernel 6.19 Introduces EEVDF Scheduler Optimizations\n\n"
                "The Linux 6.19 kernel release introduces major improvements to the Earliest Eligible Virtual "
                "Deadline First (EEVDF) CPU scheduler, reducing tail latency by 38% on multi-socket servers.\n\n"
                "Kernel developers explained that the new slice-eligibility heuristics prevent interactive "
                "threads from starving under heavy compilation and container orchestration workloads.\n\n"
                "Benchmarks conducted on 128-core AMD EPYC and Intel Xeon systems demonstrate a 24% throughput "
                "gain for PostgreSQL and Kubernetes control-plane processes."
            ),
            excerpt="The Linux 6.19 kernel release introduces major improvements to the EEVDF CPU scheduler.",
            visibility="public",
            category_slug="tech",
        )
        session.commit()

        # 1. Test du cycle de vote : +1 (like) -> 0 (enlever le like en premier) -> -1 (dislike)
        r_like = record_topic_feedback(session, user_id=user_id, topic_id=topic.id, vote=1)
        pos_after_like = r_like["positive_votes"]
        neg_after_like = r_like["negative_votes"]
        assert r_like["vote"] == 1

        # Appui sur '-' quand liké : retire le like en premier (vote = 0)
        r_unvote = record_topic_feedback(session, user_id=user_id, topic_id=topic.id, vote=0)
        assert r_unvote["previous_vote"] == 1
        assert r_unvote["vote"] == 0
        assert r_unvote["positive_votes"] == pos_after_like - 1
        assert r_unvote["negative_votes"] == neg_after_like

        # Second appui sur '-' : passe à -1 (j'aime pas)
        r_dislike = record_topic_feedback(session, user_id=user_id, topic_id=topic.id, vote=-1)
        assert r_dislike["previous_vote"] == 0
        assert r_dislike["vote"] == -1
        assert r_dislike["negative_votes"] == neg_after_like + 1

        # Remise à 0 avant suppression
        record_topic_feedback(session, user_id=user_id, topic_id=topic.id, vote=0)

        # 2. Test de la Vue Détail Approfondie (Zero-Click Briefing)
        detail = build_topic_deep_detail(
            session,
            user_id=user_id,
            topic_id=topic.id,
            auto_enrich=False,
        )
        assert detail["topic_id"] == str(topic.id)
        assert len(detail["core_synthesis"]) >= 180
        assert "EEVDF" in detail["core_synthesis"]
        assert "38%" in detail["core_synthesis"] or "24%" in detail["core_synthesis"]
        assert len(detail["key_takeaways"]) >= 2
        assert len(detail["sources_breakdown"]) >= 1
        assert len(detail["sources_breakdown"][0]["detailed_excerpt"]) >= 100
        assert isinstance(detail["subtopics"], list)

        session.delete(item)
        session.delete(topic)
        session.commit()


def test_rss_boilerplate_cleaning_subtopics_and_llm_novelty(monkeypatch) -> None:
    from morning_review.pipeline import synthesizer
    from morning_review.pipeline.ingestion import (
        clean_aggregator_noise,
        is_generic_dated_broadcast_placeholder,
    )

    # 1. Les grilles horaires radio sans description doivent être détectées comme placeholders
    assert is_generic_dated_broadcast_placeholder(
        "JOURNAL DE 09H00 du samedi 14 novembre 2026",
        "# JOURNAL DE 09H00 du samedi 14 novembre 2026 - **Source RSS** : France Culture - **Lien** : https://www.radiofrance.fr/test",
    ) is True
    assert is_generic_dated_broadcast_placeholder(
        "Une panne mondiale touche la messagerie Outlook de Microsoft",
        "Retour progressif aux services Microsoft après une panne majeure ayant touché 74 pays.",
    ) is False

    # 2. Le nettoyage d'agrégateur doit retirer les métadonnées Markdown, les accroches Medium et les encarts autopromo
    raw_noisy = (
        "[France Culture] # JOURNAL DE 09H00 du samedi 14 novembre 2026 "
        "- **Source RSS** : France Culture - À la une (Radio France) "
        "- **Lien** : https://www.radiofrance.fr/franceculture/podcasts/journal-de-9h"
    )
    cleaned = clean_aggregator_noise(raw_noisy)
    assert "Source RSS" not in cleaned
    assert "https://www.radiofrance.fr" not in cleaned

    medium_noisy = "Member-only story Hi everyone, welcome back. Today, I will be going over… (Jesse L · 17 min read)"
    cleaned_med = clean_aggregator_noise(medium_noisy)
    assert "Member-only story" not in cleaned_med
    assert "Hi everyone, welcome back" not in cleaned_med
    assert "17 min read" not in cleaned_med

    # 2b. Garde-fous du clustering hybride : deux articles "Top 10 ... Tools" ou deux sujets généraux "Physique quantique"
    # ne doivent PAS fusionner s'ils ne partagent pas d'entité spécifique commune
    from morning_review.pipeline.ingestion import compute_hybrid_story_match, extract_story_profile

    subj_iac, conc_iac = extract_story_profile(
        "Top 10 Infrastructure as Code Tools",
        "Defining your servers, networks, databases, and cloud resources in Git.",
    )
    subj_home, conc_home = extract_story_profile(
        "Top 10 Self-Hosted Home Server Tools",
        "Setting up Docker, Jellyfin, and Immich on a home server.",
    )
    matched_listicles, _ = compute_hybrid_story_match(0.75, subj_iac, conc_iac, subj_home, conc_home)
    assert matched_listicles is False

    subj_q1, conc_q1 = extract_story_profile(
        "Comment le monde quantique est devenu macroscopique",
        "Retour sur les expériences de Berkeley en 1985 sur l'effet tunnel.",
    )
    subj_q2, conc_q2 = extract_story_profile(
        "Physique quantique : la révolution invisible",
        "Des lasers aux ordinateurs quantiques, une nouvelle course technologique.",
    )
    matched_quantum, _ = compute_hybrid_story_match(0.75, subj_q1, conc_q1, subj_q2, conc_q2)
    assert matched_quantum is False

    # 3. Analyse LLM de nouveauté : si le LLM juge l'article redondant (is_novel=False), le sujet n'est pas réactivé
    snapshot = (
        "TITRE: Le scandale Nvidia met les relations entre les États-Unis et la Chine sous tension\n"
        "RÉSUMÉ: Le PDG de Nvidia Jensen Huang démissionne après des soupçons d'espionnage chinois.\n"
        "SOUS-SUJETS DÉJÀ COUVERTS:\n"
        "- [Le Monde, Radio France] Démission de Jensen Huang et enquête pour espionnage chinois."
    )
    monkeypatch.setattr(
        synthesizer,
        "_call_llm_json",
        lambda *args, **kwargs: {"is_novel": False, "new_subtopic": "", "update_summary": ""},
    )
    is_nov_false, _, upd_false = synthesizer.evaluate_topic_novelty(
        snapshot,
        new_title="Une espionne chinoise fait tomber le patron de Nvidia",
        new_text="Retour sur le scandale extraconjugal et d'espionnage qui a poussé Jensen Huang à quitter la direction de Nvidia.",
        source_title="Les Numériques",
        use_llm=True,
    )
    assert is_nov_false is False
    assert upd_false == ""

    # 4. Si le LLM détecte un vrai nouveau sous-sujet (is_novel=True), la mise à jour est formatée proprement
    monkeypatch.setattr(
        synthesizer,
        "_call_llm_json",
        lambda *args, **kwargs: {
            "is_novel": True,
            "new_subtopic": "Sanctions du Département du Commerce",
            "update_summary": "Washington annonce la suspension immédiate des licences d'exportation H200 vers trois filiales asiatiques.",
        },
    )
    is_nov_true, _, upd_true = synthesizer.evaluate_topic_novelty(
        snapshot,
        new_title="Affaire Nvidia : Washington suspend les licences d'exportation H200",
        new_text="Le Département du Commerce américain a annoncé la suspension immédiate des licences d'exportation H200 vers trois filiales asiatiques.",
        source_title="Reuters",
        use_llm=True,
    )
    assert is_nov_true is True
    assert "Sanctions du Département du Commerce" in upd_true
    assert "suspension immédiate des licences" in upd_true


def test_newsletter_optin_confirmation_detection_and_validation() -> None:
    from email.message import EmailMessage
    import httpx

    from morning_review.connectors.gmail_imap import (
        dismiss_newsletter_confirmation,
        extract_newsletter_confirmation_url,
        is_newsletter_optin_confirmation_email,
        list_newsletter_confirmations,
        validate_newsletter_confirmation_url,
    )
    from morning_review.db.models import NewsletterConfirmation

    def _make_email(sender: str, subject: str, html: str = "", text: str = "") -> EmailMessage:
        m = EmailMessage()
        m["From"] = sender
        m["Subject"] = subject
        if text:
            m.set_content(text)
        if html:
            m.add_alternative(html, subtype="html")
        return m

    # 1. Accept genuine FR/EN newsletter double opt-in emails
    msg_fr = _make_email(
        "Pour la Science <newsletter@pourlascience.fr>",
        "Confirmez votre inscription à la newsletter Pour la Science",
        html='<p>Cliquez ci-dessous :</p><a class="btn" href="https://nl.pourlascience.fr/optin/confirm?token=abc123">Valider mon inscription</a>',
    )
    assert is_newsletter_optin_confirmation_email(msg_fr, html_body='<a href="https://nl.pourlascience.fr/optin/confirm?token=abc123">Valider mon inscription</a>') is True

    msg_ghost_signup = _make_email(
        "404 Media <404-media@ghost.io>",
        "🙌 Complete your sign up to 404 Media!",
        html='<a href="https://www.404media.co/members/?token=abc&action=signup">Confirm signup</a>',
    )
    assert is_newsletter_optin_confirmation_email(
        msg_ghost_signup,
        html_body='<a href="https://www.404media.co/members/?token=abc&action=signup">Confirm signup</a>',
    ) is True
    assert (
        extract_newsletter_confirmation_url(
            '<a href="https://www.404media.co/members/?token=abc&action=signup">Confirm signup</a>'
        )
        == "https://www.404media.co/members/?token=abc&action=signup"
    )
    assert (
        extract_newsletter_confirmation_url(
            '<a href="https://www.404media.co/members/?token=abc&action=signin">Sign in</a>'
        )
        is None
    )

    # 2. Strictly reject login, magic-link, password reset, security, OAuth, and retention emails
    for bad_sender, bad_subj, bad_body in [
        ("Substack <no-reply@substack.com>", "Sign in to Substack", "Click this magic link to sign in."),
        ("GitHub <noreply@github.com>", "Confirm your email address", "Confirm your GitHub account."),
        ("Example <security@example.com>", "Reset your password", "Click to reset password."),
        ("Newsletter <news@example.com>", "Confirm you want to stay subscribed", "Click to stay subscribed."),
    ]:
        bad_msg = _make_email(bad_sender, bad_subj, text=bad_body)
        assert is_newsletter_optin_confirmation_email(bad_msg, text_body=bad_body) is False

    # 3. Extract confirmation CTA while strictly ignoring unsubscribe/privacy/social links
    sample_html = """
    <html><body>
      <a href="https://twitter.com/example">Follow us</a>
      <a class="button" style="background:#2563eb" href="https://news.example.org/subscribe/confirm?t=999">
        Oui, je confirme mon inscription
      </a>
      <a href="https://news.example.org/unsubscribe?t=999">Se désabonner / Unsubscribe</a>
    </body></html>
    """
    extracted = extract_newsletter_confirmation_url(sample_html)
    assert extracted == "https://news.example.org/subscribe/confirm?t=999"

    # 4. Validate HTTP GET, RFC 9110 two-step POST form, and strict CAPTCHA non-bypass fallback
    post_submitted = {"count": 0}

    def _mock_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/direct-confirm":
            return httpx.Response(
                200,
                text="<html><body><h1>Subscription confirmed! Thank you for subscribing.</h1></body></html>",
            )
        if path == "/two-step" and request.method == "GET":
            return httpx.Response(
                200,
                text=(
                    '<html><body><form method="post" action="/two-step/submit">'
                    '<input type="hidden" name="csrf" value="tok42"/>'
                    '<button type="submit">Confirm subscription</button>'
                    "</form></body></html>"
                ),
            )
        if path == "/two-step/submit" and request.method == "POST":
            post_submitted["count"] += 1
            return httpx.Response(200, text="<html><body><p>Votre inscription est confirmée !</p></body></html>")
        if path == "/captcha-gate":
            return httpx.Response(
                200,
                text=(
                    '<html><body><div class="cf-turnstile" data-sitekey="1x0000"></div>'
                    '<form method="post" action="/two-step/submit"><button>Confirm subscription</button></form>'
                    "</body></html>"
                ),
            )
        return httpx.Response(404, text="Not found")

    with httpx.Client(transport=httpx.MockTransport(_mock_handler), follow_redirects=True) as mock_client:
        res_get = validate_newsletter_confirmation_url("https://news.example.org/direct-confirm", http_client=mock_client)
        assert res_get["status"] == "confirmed"

        res_post = validate_newsletter_confirmation_url("https://news.example.org/two-step", http_client=mock_client)
        assert res_post["status"] == "confirmed"
        assert post_submitted["count"] == 1

        # CAPTCHA gate MUST return manual_required immediately without submitting the form
        res_captcha = validate_newsletter_confirmation_url("https://news.example.org/captcha-gate", http_client=mock_client)
        assert res_captcha["status"] == "manual_required"
        assert post_submitted["count"] == 1

    # 5. DB persistence, dismissal, and Daily Digest inclusion
    from pathlib import Path

    user_id = init_db()
    with SessionLocal() as session:
        session.query(NewsletterConfirmation).filter(
            NewsletterConfirmation.message_id == "<test-optin-fixture@morning-review.local>"
        ).delete()
        session.commit()

        conf = NewsletterConfirmation(
            user_id=user_id,
            message_id="<test-optin-fixture@morning-review.local>",
            sender_name="Cafzone Newsletter",
            sender_email="newsletter@cafzone.example",
            subject="Confirmez votre abonnement",
            confirmation_url="https://cafzone.example/confirm?token=fixture",
            status="manual_required",
            status_detail="CAPTCHA détecté — validation manuelle requise",
        )
        session.add(conf)
        session.commit()

        active_list = list_newsletter_confirmations(session, user_id=user_id)
        assert any(c["id"] == str(conf.id) and c["status"] == "manual_required" for c in active_list)

        digest = generate_daily_digest(session, user_id=user_id)
        report_md = Path(digest["local_report_path"]).read_text(encoding="utf-8")
        assert "Inscriptions Newsletters" in report_md
        assert "https://cafzone.example/confirm?token=fixture" in report_md

        dismissed = dismiss_newsletter_confirmation(session, user_id=user_id, conf_id=conf.id)
        assert dismissed["status"] == "dismissed"
        active_after = list_newsletter_confirmations(session, user_id=user_id, include_dismissed=False)
        assert all(c["id"] != str(conf.id) for c in active_after)

        session.delete(conf)
        session.commit()
        generate_daily_digest(session, user_id=user_id)


def test_daily_digest_smtp_idempotency_and_rearm_hook() -> None:
    import json
    import subprocess
    from datetime import date
    from pathlib import Path
    from morning_review.core.config import PROJECT_ROOT, settings
    from morning_review.db.models import UserDigest

    if not settings.gmail_account:
        settings.gmail_account = "mathieuv.alterai@gmail.com"
    if not settings.gmail_pwd:
        settings.gmail_pwd = "test-app-password-ci"

    sent_messages: list[object] = []

    class _DummySMTP:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def login(self, user: str, pwd: str) -> None:
            assert "@" in user
            assert len(pwd) >= 8

        def send_message(self, msg) -> None:
            sent_messages.append(msg)

    user_id = init_db()
    test_date = date(2026, 1, 2)
    with SessionLocal() as session:
        session.query(UserDigest).filter(
            UserDigest.user_id == user_id,
            UserDigest.digest_date == test_date,
        ).delete()
        session.commit()

        res_first = generate_daily_digest(
            session,
            user_id=user_id,
            digest_date=test_date,
            max_topics=5,
            send_email=True,
            recipient_email="mathieu.veron@gmail.com",
            smtp_factory=_DummySMTP,
        )
        assert res_first["email_status"] == "sent"
        assert res_first["emailed_to"] == "mathieu.veron@gmail.com"
        assert len(sent_messages) == 1
        msg_obj = sent_messages[0]
        assert msg_obj["To"] == "mathieu.veron@gmail.com"
        assert "Morning Review" in msg_obj["Subject"]
        assert not msg_obj["Subject"].startswith("☕")
        assert msg_obj["Date"] is not None
        assert msg_obj["Message-ID"] is not None and msg_obj["Message-ID"].endswith("@gmail.com>")
        assert "mathieuvalterai@gmail.com" in msg_obj["From"]
        assert msg_obj.is_multipart()
        raw_mime = msg_obj.as_string()
        assert "http://localhost" not in raw_mime
        assert "topic_id" not in raw_mime

        from morning_review.connectors.gmail_smtp import build_digest_html, build_digest_plain_text

        sample_cat = {
            "tech": [
                {
                    "title": "Sujet de test digest",
                    "primary_url": "https://example.com/article",
                    "summary": "Résumé synthétique complet dans le corps.",
                    "relevance_score": 4.2,
                    "source_count": 2,
                    "subtopics": [
                        {"title": "Sous-sujet redondant", "summary": "Ne doit pas apparaître dans le mail."}
                    ],
                    "sources": [{"source_title": "Example"}],
                }
            ]
        }
        rendered_html = build_digest_html(
            target_date=test_date,
            by_category=sample_cat,
            confirmations=[],
            ui_url="https://review.perso.frenchtoast.dev",
        )
        rendered_plain = build_digest_plain_text(
            target_date=test_date,
            by_category=sample_cat,
            confirmations=[],
            ui_url="https://review.perso.frenchtoast.dev",
        )
        assert "Résumé synthétique complet dans le corps." in rendered_html
        assert "Sous-sujet redondant" not in rendered_html
        assert "Sous-sujet redondant" not in rendered_plain

        # 2e appel le même jour sans force_send : idempotent (aucun nouvel e-mail envoyé)
        res_second = generate_daily_digest(
            session,
            user_id=user_id,
            digest_date=test_date,
            max_topics=5,
            send_email=True,
            recipient_email="mathieu.veron@gmail.com",
            smtp_factory=_DummySMTP,
        )
        assert res_second["email_status"] == "already_sent"
        assert len(sent_messages) == 1

        # Nettoyage du digest de test (et du rapport local de date fictive)
        session.query(UserDigest).filter(
            UserDigest.user_id == user_id,
            UserDigest.digest_date == test_date,
        ).delete()
        session.commit()
        fake_report = Path(res_first["local_report_path"])
        if fake_report.exists():
            fake_report.unlink()

    # Vérification du registre .agents/schedules.json et du hook .agents/rearm_hook.py
    sched_path = PROJECT_ROOT / ".agents" / "schedules.json"
    assert sched_path.exists()
    sched_data = json.loads(sched_path.read_text(encoding="utf-8"))
    assert len(sched_data.get("daemons", [])) >= 1
    assert len(sched_data.get("schedules", [])) >= 1
    assert sched_data["schedules"][0]["cronExpression"] in ("30 6 * * *", "15 */2 * * *")

    hook_proc = subprocess.run(
        ["python3", "./rearm_hook.py"],
        cwd=str(PROJECT_ROOT / ".agents"),
        input=json.dumps({"hookEventName": "PreInvocation", "conversationId": "pytest-conv", "lastUserInput": "rearm"}),
        text=True,
        capture_output=True,
        check=True,
    )
    hook_out = json.loads(hook_proc.stdout)
    any_enabled = any(d.get("enabled", True) for d in sched_data.get("daemons", [])) or any(
        s.get("enabled", True) for s in sched_data.get("schedules", [])
    )
    if any_enabled:
        assert "injectSteps" in hook_out
    else:
        assert hook_out == {}


def test_k8s_and_b2_cnpg_configuration() -> None:
    import yaml
    from morning_review.core.config import PROJECT_ROOT, Settings

    s = Settings(
        DATABASE_URL="postgresql://morning:secret@morning-review-pg-rw:5432/morning_review",
        B2_ENDPOINT_URL="https://s3.eu-central-003.backblazeb2.com",
        B2_REGION="eu-central-003",
        B2_BUCKET="frenchtoast-perso",
        B2_KEY_ID="003keyid",
        B2_APPLICATION_KEY="K003secret",
    )
    assert s.effective_database_url == "postgresql+psycopg://morning:secret@morning-review-pg-rw:5432/morning_review"
    assert s.effective_s3_endpoint == "https://s3.eu-central-003.backblazeb2.com"
    assert s.effective_s3_region == "eu-central-003"
    assert s.effective_s3_bucket == "frenchtoast-perso"
    assert s.effective_s3_access_key == "003keyid"
    assert s.effective_s3_secret_key == "K003secret"
    assert s.effective_s3_prefix == "morning-review"

    k8s_deploy_path = PROJECT_ROOT / "k8s" / "deployment.yaml"
    k8s_argocd_path = PROJECT_ROOT / "k8s" / "argocd-application.yaml"
    assert k8s_deploy_path.exists()
    assert k8s_argocd_path.exists()

    docs = [d for d in yaml.safe_load_all(k8s_deploy_path.read_text(encoding="utf-8")) if d]
    kinds = {(d["kind"], d["metadata"]["name"]) for d in docs}
    assert ("Namespace", "morning-review") in kinds
    assert ("Cluster", "morning-review-pg") in kinds
    assert ("Deployment", "morning-review") in kinds
    assert ("CronJob", "morning-review-refresh-hourly") in kinds
    assert ("CronJob", "morning-review-daily-06h30") in kinds
    assert ("Middleware", "morning-review-basic-auth") in kinds
    assert ("Gateway", "morning-review-gateway") in kinds
    assert ("HTTPRoute", "morning-review-route") in kinds

    cron_daily = next(d for d in docs if d["kind"] == "CronJob" and d["metadata"]["name"] == "morning-review-daily-06h30")
    assert cron_daily["spec"]["schedule"] == "30 6 * * *"
    assert cron_daily["spec"]["timeZone"] == "Europe/Paris"

    http_route = next(d for d in docs if d["kind"] == "HTTPRoute")
    assert "review.perso.frenchtoast.dev" in http_route["spec"]["hostnames"]


def test_digest_memory_and_longform_scoring_and_edge_push() -> None:
    from datetime import datetime, timezone
    from morning_review.db.models import Topic, UserDigest
    from morning_review.pipeline.search_and_profile import compute_topic_relevance

    now = datetime.now(timezone.utc)
    quanta_topic = Topic(
        title="How MathematiciansSolved the Geometric Langlands Conjecture",
        summary="A 1,000-page proof in algebraic geometry bridges number theory and harmonic analysis.",
        category_slug="science",
        primary_url="https://www.quantamagazine.org/geometric-langlands-proof-20261118/",
        source_count=1,
        first_seen_at=now,
        last_seen_at=now,
    )
    blurb_topic = Topic(
        title="Tech Company Releases Minor App Update",
        summary="Short blurb.",
        category_slug="science",
        primary_url="https://example.com/minor-update",
        source_count=3,
        first_seen_at=now,
        last_seen_at=now,
    )
    score_quanta = compute_topic_relevance(quanta_topic, None, None)
    score_blurb = compute_topic_relevance(blurb_topic, None, None)
    assert score_quanta > score_blurb






