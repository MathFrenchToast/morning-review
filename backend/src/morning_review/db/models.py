from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

from pgvector.sqlalchemy import HALFVEC
from sqlalchemy import (
     Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_uuid() -> uuid.UUID:
    return uuid.uuid4()


class Base(DeclarativeBase):
    pass


class User(Base):
    """Utilisateur identifié par un UUID unique non auto-incrémenté.
    Prêt pour l'accostage OIDC obligatoire Post-MVP (oidc_issuer + oidc_sub).
    """

    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("oidc_issuer", "oidc_sub", name="uq_users_oidc_identity"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=new_uuid,
    )
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(120), default="Owner")
    oidc_issuer: Mapped[str | None] = mapped_column(String(512), nullable=True)
    oidc_sub: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class UserCategory(Base):
    """Catégories de rangement rattachées à user_id (tech, perso, science...)."""

    __tablename__ = "user_categories"
    __table_args__ = (
        UniqueConstraint("user_id", "slug", name="uq_user_categories_user_slug"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=new_uuid,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    slug: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Source(Base):
    """Catalogue des flux et ressources (RSS, sites web, newsletters, connecteurs MCP).
    Les sources publiques (is_public=True) sont mutualisées entre utilisateurs sans doublon.
    """

    __tablename__ = "sources"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=new_uuid,
    )
    kind: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        index=True,
        doc="rss | web | newsletter | mcp_auth",
    )
    url: Mapped[str] = mapped_column(
        String(1024),
        unique=True,
        nullable=False,
        doc="URL du flux RSS/Web, mcp://..., ou mailto:/list-id: pour une newsletter",
    )
    site_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    sender_email: Mapped[str | None] = mapped_column(String(320), nullable=True, index=True)
    list_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    is_public: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    etag: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_modified: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    item_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class UserSubscription(Base):
    """Liste des flux et ressources maintenus pour un user_id donné."""

    __tablename__ = "user_subscriptions"
    __table_args__ = (
        UniqueConstraint("user_id", "source_id", name="uq_user_subscriptions_user_source"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=new_uuid,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    category_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("user_categories.id", ondelete="SET NULL"),
        nullable=True,
    )
    category_slug: Mapped[str] = mapped_column(String(64), default="tech", nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    discovery_method: Mapped[str] = mapped_column(
        String(32),
        default="yaml_seed",
        nullable=False,
        doc="yaml_seed | gmail_auto | api | mcp_edge",
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Topic(Base):
    """Sujet unique (topic_id UUID) fédérant 1..N articles/mentions issus de sources multiples."""

    __tablename__ = "topics"
    __table_args__ = (
        Index(
            "ix_topics_centroid_hnsw",
            "centroid_embedding",
            postgresql_using="hnsw",
            postgresql_with={"m": 16, "ef_construction": 64},
            postgresql_ops={"centroid_embedding": "halfvec_cosine_ops"},
        ),
        Index("ix_topics_fts_gin", "fts", postgresql_using="gin"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=new_uuid,
    )
    visibility: Mapped[str] = mapped_column(String(16), default="public", nullable=False)
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    subtopics_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB, nullable=True)
    category_slug: Mapped[str] = mapped_column(String(64), default="tech", nullable=False, index=True)
    primary_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    source_count: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    centroid_embedding: Mapped[Any | None] = mapped_column(HALFVEC(384), nullable=True)
    fts: Mapped[Any | None] = mapped_column(TSVECTOR, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        index=True,
        doc="Date de la première source (MIN published_at des articles rattachés)",
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        index=True,
        doc="Date de la source la plus récente (MAX published_at des articles rattachés)",
    )


class CanonicalItem(Base):
    """Article ou élément de contenu dédoublonné, dont le Markdown complet est stocké sur S3."""

    __tablename__ = "canonical_items"
    __table_args__ = (
        CheckConstraint(
            "(visibility = 'public' AND user_id IS NULL) OR (visibility = 'private' AND user_id IS NOT NULL)",
            name="ck_canonical_items_visibility_user",
        ),
        Index(
            "ix_canonical_items_embedding_hnsw",
            "embedding",
            postgresql_using="hnsw",
            postgresql_with={"m": 16, "ef_construction": 64},
            postgresql_ops={"embedding": "halfvec_cosine_ops"},
        ),
        Index("ix_canonical_items_fts_gin", "fts", postgresql_using="gin"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=new_uuid,
    )
    visibility: Mapped[str] = mapped_column(String(16), default="public", nullable=False, index=True)
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    topic_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("topics.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    canonical_url: Mapped[str] = mapped_column(String(1024), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    author: Mapped[str | None] = mapped_column(String(255), nullable=True)
    excerpt: Mapped[str | None] = mapped_column(Text, nullable=True)
    category_slug: Mapped[str] = mapped_column(String(64), default="tech", nullable=False, index=True)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    s3_key: Mapped[str] = mapped_column(String(512), nullable=False)
    language: Mapped[str | None] = mapped_column(String(16), nullable=True)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    embedding: Mapped[Any | None] = mapped_column(HALFVEC(384), nullable=True)
    embedding_model_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    fts: Mapped[Any | None] = mapped_column(TSVECTOR, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class SourceMention(Base):
    """Occurrence d'un item dans une source (ex: lien cité dans une newsletter Gmail ou flux RSS)."""

    __tablename__ = "source_mentions"
    __table_args__ = (
        UniqueConstraint("source_id", "external_id", name="uq_source_mentions_source_extid"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=new_uuid,
    )
    source_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    item_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("canonical_items.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    external_id: Mapped[str] = mapped_column(String(512), nullable=False)
    raw_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    curator_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class TopicItem(Base):
    """Table de liaison N-N entre un sujet unique (Topic) et les CanonicalItems qui le composent."""

    __tablename__ = "topic_items"

    topic_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("topics.id", ondelete="CASCADE"),
        primary_key=True,
    )
    item_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("canonical_items.id", ondelete="CASCADE"),
        primary_key=True,
    )
    similarity_score: Mapped[float] = mapped_column(Float, default=1.0, nullable=False)
    source_role: Mapped[str] = mapped_column(String(32), default="primary", nullable=False)


class UserTopicFeedback(Base):
    """Feedback explicite d'auto-profilage par user_id (+1 pertinent, -1 non pertinent)."""

    __tablename__ = "user_topic_feedback"

    user_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    topic_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("topics.id", ondelete="CASCADE"),
        primary_key=True,
    )
    vote: Mapped[int] = mapped_column(SmallInteger, nullable=False, doc="+1 ou -1")
    voted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class UserTopicState(Base):
    """État de lecture Inbox Zero ('mark as read' / archivé) et suivi des nouveautés par (user_id, topic_id)."""

    __tablename__ = "user_topic_states"
    __table_args__ = (
        Index("ix_user_topic_states_user_archived", "user_id", "is_archived"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    topic_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("topics.id", ondelete="CASCADE"),
        primary_key=True,
    )
    is_archived: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        nullable=False,
        index=True,
        doc="True si le sujet est marqué comme lu/archivé (masqué du flux principal)",
    )
    archived_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    archived_source_count: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    archived_summary_snapshot: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        doc="Snapshot du résumé des éléments clés au moment où le sujet a été marqué comme lu",
    )
    has_updates: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False,
        index=True,
        doc="True si le sujet archivé a été réactivé suite à un nouvel article apportant des faits inédits",
    )
    updates_summary: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        doc="Résumé des éléments nouveaux (non présents dans archived_summary_snapshot) ayant déclenché la réactivation",
    )
    new_articles_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    reactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class UserProfile(Base):
    """Vecteur centroïde d'intérêt (Rocchio) et affinités par source/catégorie pour un user_id."""

    __tablename__ = "user_profiles"

    user_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    interest_embedding: Mapped[Any | None] = mapped_column(HALFVEC(384), nullable=True)
    source_affinities: Mapped[dict[str, float]] = mapped_column(JSONB, default=dict, nullable=False)
    category_affinities: Mapped[dict[str, float]] = mapped_column(JSONB, default=dict, nullable=False)
    positive_votes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    negative_votes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class UserDigest(Base):
    """Rapport quotidien (Morning Review) généré pour un user_id."""

    __tablename__ = "user_digests"
    __table_args__ = (
        UniqueConstraint("user_id", "digest_date", name="uq_user_digests_user_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=new_uuid,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    digest_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    s3_key: Mapped[str] = mapped_column(String(512), nullable=False)
    topics_json: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list, nullable=False)
    emailed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    emailed_to: Mapped[str | None] = mapped_column(String(320), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class NewsletterConfirmation(Base):
    """Suivi idempotent des emails de confirmation d'inscription newsletter (double opt-in)."""

    __tablename__ = "newsletter_confirmations"
    __table_args__ = (
        UniqueConstraint("user_id", "message_id", name="uq_newsletter_confirmations_user_msgid"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=new_uuid,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    message_id: Mapped[str] = mapped_column(String(512), nullable=False)
    sender_name: Mapped[str] = mapped_column(String(255), nullable=False)
    sender_email: Mapped[str] = mapped_column(String(320), nullable=False)
    subject: Mapped[str] = mapped_column(String(512), nullable=False)
    confirmation_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        default="manual_required",
        nullable=False,
        index=True,
        doc="confirmed | manual_required | dismissed",
    )
    status_detail: Mapped[str | None] = mapped_column(String(512), nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
