import json
import logging
import tempfile
from pathlib import Path

from kafka import KafkaConsumer
from pydantic import ValidationError

from .config import settings
from .models import SrtGenerationRequest, SrtGenerationResult, JobStatus
from .transcriber import transcribe
from .srt_writer import write_srt_file
from .producer import publish_result
from .storage import download_source_file, upload_result_file, delete_source_file, cleanup_temp_file

logger = logging.getLogger(__name__)


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        settings.kafka_topic_requests,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def handle_message(request: SrtGenerationRequest) -> None:
    logger.info("Job recibido: %s", request.job_id)

    try:
        source_path = download_source_file(request.object_key)
    except Exception as e:
        logger.exception("No se pudo acceder al archivo fuente del job %s: %s", request.job_id, e)
        publish_result(SrtGenerationResult(
            job_id=request.job_id, status=JobStatus.FAILED,
            error=f"No se pudo acceder al archivo fuente: {e}",
        ))
        return

    if settings.storage_provider == "local" and not source_path.exists():
        logger.error("El archivo no existe para el job %s: %s", request.job_id, source_path)
        publish_result(SrtGenerationResult(
            job_id=request.job_id, status=JobStatus.FAILED,
            error="Archivo fuente no encontrado en el storage",
        ))
        return

    try:
        result = transcribe(str(source_path), source_language=request.source_language)
    except Exception as e:
        logger.exception("Error transcribiendo el job %s: %s", request.job_id, e)
        publish_result(SrtGenerationResult(
            job_id=request.job_id, status=JobStatus.FAILED,
            error=f"Error durante la transcripción: {e}",
        ))
        cleanup_temp_file(source_path)
        return

    srt_object_key = f"results/{request.job_id}/subtitles.srt"

    if settings.storage_provider == "r2":
        local_output = Path(tempfile.gettempdir()) / f"{request.job_id}.srt"
    else:
        local_output = Path(settings.storage_base_path) / srt_object_key

    write_srt_file(result.segments, local_output)
    upload_result_file(local_output, srt_object_key)

    logger.info(
        "SRT generado para el job %s (idioma detectado: %s)",
        request.job_id, result.detected_language,
    )

    publish_result(SrtGenerationResult(
        job_id=request.job_id,
        status=JobStatus.COMPLETED,
        srt_object_key=srt_object_key,
        detected_language=result.detected_language,
    ))

    # limpieza: el vídeo original ya no hace falta
    delete_source_file(request.object_key, source_path)
    cleanup_temp_file(source_path)


def start_consuming() -> None:
    consumer = create_consumer()
    logger.info(
        "Escuchando el tópico '%s' en %s...",
        settings.kafka_topic_requests,
        settings.kafka_bootstrap_servers,
    )

    for message in consumer:
        try:
            request = SrtGenerationRequest.model_validate(message.value)
            handle_message(request)
        except ValidationError as e:
            logger.error("Mensaje con formato inválido, se ignora: %s", e)
        except Exception as e:
            logger.exception("Error inesperado procesando el mensaje: %s", e)