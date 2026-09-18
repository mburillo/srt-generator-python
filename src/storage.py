import logging
import tempfile
from pathlib import Path

from .config import settings

logger = logging.getLogger(__name__)

_s3_client = None


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        import boto3
        _s3_client = boto3.client(
            "s3",
            endpoint_url=settings.r2_endpoint,
            aws_access_key_id=settings.r2_access_key,
            aws_secret_access_key=settings.r2_secret_key,
            region_name="auto",
        )
    return _s3_client


def download_source_file(object_key: str) -> Path:
    """
    Devuelve una ruta local del archivo fuente para que ffmpeg/Whisper lo procesen.
    En modo R2, lo descarga primero a un archivo temporal.
    En modo local, ya está en el filesystem compartido, no hay que hacer nada.
    """
    if settings.storage_provider == "r2":
        client = _get_s3_client()
        suffix = Path(object_key).suffix
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp_path = Path(tmp_file.name)
        tmp_file.close()

        logger.info("Descargando de R2: %s -> %s", object_key, tmp_path)
        client.download_file(settings.r2_bucket, object_key, str(tmp_path))
        return tmp_path

    return Path(settings.storage_base_path) / object_key


def upload_result_file(local_path: Path, object_key: str) -> None:
    """Sube el .srt generado. En modo local ya se escribió directamente en su sitio."""
    if settings.storage_provider == "r2":
        client = _get_s3_client()
        logger.info("Subiendo resultado a R2: %s -> %s", local_path, object_key)
        client.upload_file(str(local_path), settings.r2_bucket, object_key)


def delete_source_file(object_key: str, local_path: Path) -> None:
    """
    Borra el vídeo original una vez generado el SRT (ya no se necesita).
    En R2 esto es la limpieza a nivel de aplicación; la regla de lifecycle
    del bucket actúa como red de seguridad si esto llegara a fallar.
    """
    if settings.storage_provider == "r2":
        try:
            client = _get_s3_client()
            client.delete_object(Bucket=settings.r2_bucket, Key=object_key)
            logger.info("Vídeo original borrado de R2: %s", object_key)
        except Exception as e:
            logger.warning("No se pudo borrar el vídeo original de R2 (%s): %s", object_key, e)
    else:
        try:
            if local_path.exists():
                local_path.unlink()
                logger.info("Vídeo original borrado localmente: %s", local_path)
        except Exception as e:
            logger.warning("No se pudo borrar el vídeo original local (%s): %s", local_path, e)


def cleanup_temp_file(path: Path) -> None:
    """Limpia el archivo temporal descargado de R2 para procesar (solo aplica en modo R2)."""
    if settings.storage_provider == "r2":
        try:
            if path.exists():
                path.unlink()
        except Exception as e:
            logger.warning("No se pudo limpiar el archivo temporal %s: %s", path, e)