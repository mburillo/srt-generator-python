import json
import logging

from kafka import KafkaProducer

from .config import settings
from .models import SrtGenerationResult

logger = logging.getLogger(__name__)

_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
        )
    return _producer


def publish_result(result: SrtGenerationResult) -> None:
    producer = get_producer()
    payload = result.model_dump(by_alias=True, exclude_none=False)

    future = producer.send(
        settings.kafka_topic_results,
        key=result.job_id,
        value=payload,
    )

    try:
        record_metadata = future.get(timeout=10)
        logger.info(
            "Resultado publicado para job %s en partición %d, offset %d",
            result.job_id, record_metadata.partition, record_metadata.offset,
        )
    except Exception as e:
        logger.exception("Error publicando resultado para job %s: %s", result.job_id, e)