from pathlib import Path

from .transcriber import TranscriptSegment


def _format_timestamp(seconds: float) -> str:
    total_ms = round(seconds * 1000)
    hours, remainder_ms = divmod(total_ms, 3_600_000)
    minutes, remainder_ms = divmod(remainder_ms, 60_000)
    secs, millis = divmod(remainder_ms, 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def segments_to_srt(segments: list[TranscriptSegment]) -> str:
    lines = []
    for index, segment in enumerate(segments, start=1):
        start = _format_timestamp(segment.start)
        end = _format_timestamp(segment.end)
        lines.append(str(index))
        lines.append(f"{start} --> {end}")
        lines.append(segment.text)
        lines.append("")  # línea en blanco entre bloques, requerida por el formato SRT

    return "\n".join(lines)


def write_srt_file(segments: list[TranscriptSegment], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = segments_to_srt(segments)
    output_path.write_text(content, encoding="utf-8")