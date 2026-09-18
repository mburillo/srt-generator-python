from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_requests: str = "srt-generation-requests"
    kafka_topic_results: str = "srt-generation-results"
    kafka_consumer_group: str = "srt-worker-group"
    storage_base_path: str = "/home/magan/subtitles-storage"
    whisper_model_size: str = "small"
    whisper_device: str = "cpu"
    whisper_compute_type: str = "int8"

    storage_provider: str = "local"  # "local" o "r2"
    r2_endpoint: str = ""
    r2_access_key: str = ""
    r2_secret_key: str = ""
    r2_bucket: str = ""
    
    class Config:
        env_file = ".env"

settings = Settings()