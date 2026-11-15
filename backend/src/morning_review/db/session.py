from __future__ import annotations

from collections.abc import Generator
from uuid import UUID

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker

from morning_review.core.config import settings
from morning_review.db.models import Base, User, UserProfile

engine = create_engine(settings.effective_database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def init_db() -> UUID:
    """Active pgvector, crée les tables/index et garantit l'existence du user_id bootstrap MVP."""
    with engine.begin() as conn:
        has_vector = conn.scalar(
            text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
        )
        if not has_vector:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        Base.metadata.create_all(bind=conn)
        conn.execute(text("ALTER TABLE topics ADD COLUMN IF NOT EXISTS subtopics_json JSONB"))
        conn.execute(text("ALTER TABLE user_digests ADD COLUMN IF NOT EXISTS emailed_at TIMESTAMPTZ"))
        conn.execute(text("ALTER TABLE user_digests ADD COLUMN IF NOT EXISTS emailed_to VARCHAR(320)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_topics_first_seen_at ON topics (first_seen_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_topics_last_seen_at ON topics (last_seen_at)"))

    with SessionLocal() as session:
        user = session.get(User, settings.bootstrap_user_id)
        if user is None:
            user = User(
                id=settings.bootstrap_user_id,
                email=settings.effective_bootstrap_email,
                display_name="Bootstrap Owner",
            )
            session.add(user)
            session.add(UserProfile(user_id=settings.bootstrap_user_id))
            session.commit()
        return user.id


def get_db() -> Generator[Session, None, None]:
    with SessionLocal() as session:
        yield session
