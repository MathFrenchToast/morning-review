from __future__ import annotations

import hashlib
import logging
from datetime import date
from pathlib import Path
from uuid import UUID

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from morning_review.core.config import PROJECT_ROOT, settings

logger = logging.getLogger(__name__)


class ContentStorage:
    """Stockage Texte Adressable par Contenu (S3 compatible avec repli local transparent).

    Clés S3 :
    - Contenu public mutualisé : `public/items/<sha256[:2]>/<sha256>.md`
    - Contenu privé par user_id : `users/<user_id>/items/<sha256[:2]>/<sha256>.md`
    - Rapport quotidien : `users/<user_id>/digests/<YYYY>/<MM>/<YYYY-MM-DD>.md`
    """

    def __init__(self) -> None:
        self.bucket = settings.s3_bucket_name
        self.local_fallback_dir = PROJECT_ROOT / ".data" / "s3" / self.bucket
        self._bucket_ensured = False
        self._s3 = boto3.client(
            "s3",
            endpoint_url=settings.aws_endpoint_url_s3,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": settings.s3_addressing_style},
                connect_timeout=3,
                read_timeout=5,
                retries={"max_attempts": 2},
            ),
        )

    @staticmethod
    def compute_sha256(content: str) -> str:
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    @staticmethod
    def build_item_key(
        content_sha256: str,
        visibility: str,
        user_id: UUID | None = None,
    ) -> str:
        prefix = content_sha256[:2]
        if visibility == "public":
            return f"public/items/{prefix}/{content_sha256}.md"
        if user_id is None:
            raise ValueError("Un item privé requiert obligatoirement un user_id (UUID).")
        return f"users/{user_id}/items/{prefix}/{content_sha256}.md"

    @staticmethod
    def build_digest_key(user_id: UUID, digest_date: date) -> str:
        return (
            f"users/{user_id}/digests/"
            f"{digest_date.year:04d}/{digest_date.month:02d}/{digest_date.isoformat()}.md"
        )

    def ensure_bucket(self) -> bool:
        if self._bucket_ensured:
            return True
        try:
            self._s3.head_bucket(Bucket=self.bucket)
            self._bucket_ensured = True
            return True
        except ClientError:
            try:
                self._s3.create_bucket(Bucket=self.bucket)
                self._bucket_ensured = True
                return True
            except (ClientError, BotoCoreError) as exc:
                logger.warning("S3 bucket creation fallback to local fs: %s", exc)
                return False
        except BotoCoreError as exc:
            logger.warning("S3 unreachable, using local fallback: %s", exc)
            return False

    def put_markdown(self, key: str, markdown_text: str) -> str:
        body = markdown_text.encode("utf-8")
        if self.ensure_bucket():
            try:
                self._s3.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=body,
                    ContentType="text/markdown; charset=utf-8",
                )
                return key
            except (ClientError, BotoCoreError) as exc:
                logger.warning("S3 put_object failed (%s), writing to local mirror", exc)

        target = self.local_fallback_dir / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        return key

    def get_markdown(self, key: str) -> str | None:
        if self.ensure_bucket():
            try:
                resp = self._s3.get_object(Bucket=self.bucket, Key=key)
                return resp["Body"].read().decode("utf-8")
            except (ClientError, BotoCoreError):
                pass

        target = self.local_fallback_dir / key
        if target.exists():
            return target.read_text(encoding="utf-8")
        return None


storage = ContentStorage()
