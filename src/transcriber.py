import logging
from dataclasses import dataclass

from faster_whisper import WhisperModel

from .config import settings

logger = logging.getLogger(__name__)

_model: WhisperModel | None = None


def get_model() -> WhisperModel:
    global _model
    if _model is None:
        logger.info("Cargando modelo Whisper '%s' (esto puede tardar la primera vez)...", settings.whisper_model_size)
        _model = WhisperModel(
            settings.whisper_model_size,
            device=settings.whisper_device,
            compute_type=settings.whisper_compute_type,
        )
        logger.info("Modelo cargado.")
    return _model


@dataclass
class TranscriptSegment:
    start: float
    end: float
    text: str


@dataclass
class TranscriptionResult:
    detected_language: str
    segments: list[TranscriptSegment]


def transcribe(audio_path: str, source_language: str | None = None) -> TranscriptionResult:
    model = get_model()

    segments_iter, info = model.transcribe(
        audio_path,
        language=source_language,  # None = autodetección
        task="transcribe",
        vad_filter=True,
    )

    segments = [
        TranscriptSegment(start=seg.start, end=seg.end, text=seg.text.strip())
        for seg in segments_iter
    ]

    logger.info(
        "Transcripción completada. Idioma detectado: %s (probabilidad %.2f), %d segmentos",
        info.language, info.language_probability, len(segments),
    )

    return TranscriptionResult(detected_language=info.language, segments=segments)