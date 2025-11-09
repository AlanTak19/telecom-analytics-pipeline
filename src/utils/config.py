"""
Configuration management for Telecom Analytics Pipeline
Loads settings from .env file and provides easy access
"""

import os
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class KafkaConfig:
    """Kafka configuration"""
    bootstrap_servers: str
    topic: str
    
    @classmethod
    def from_env(cls):
        return cls(
            bootstrap_servers=os.getenv('KAFKA_BROKER', 'localhost:9092'),
            topic=os.getenv('KAFKA_TOPIC', 'telecom_events')
        )


@dataclass
class DatabaseConfig:
    """PostgreSQL configuration"""
    host: str
    port: int
    database: str
    user: str
    password: str
    
    @classmethod
    def from_env(cls):
        return cls(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
            database=os.getenv('POSTGRES_DB', 'telecom_analytics'),
            user=os.getenv('POSTGRES_USER', 'telecom_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'SecurePassword123!')
        )
    
    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class ProducerConfig:
    """Event producer configuration"""
    events_per_second: int
    delay: float
    regions: List[str]
    
    @classmethod
    def from_env(cls):
        return cls(
            events_per_second=int(os.getenv('EVENTS_PER_SECOND', '10')),
            delay=float(os.getenv('PRODUCER_DELAY', '0.1')),
            regions=os.getenv(
                'REGIONS',
                'Almaty,Astana,Shymkent,Aktobe,Karaganda,Taraz,Pavlodar,Oskemen'
            ).split(',')
        )


class Config:
    """Main configuration class"""
    
    def __init__(self):
        self.kafka = KafkaConfig.from_env()
        self.database = DatabaseConfig.from_env()
        self.producer = ProducerConfig.from_env()
    
    def __repr__(self):
        return (
            f"Config(\n"
            f"  Kafka: {self.kafka.bootstrap_servers} -> {self.kafka.topic}\n"
            f"  Database: {self.database.host}:{self.database.port}/{self.database.database}\n"
            f"  Producer: {self.producer.events_per_second} events/sec\n"
            f")"
        )


# Global config instance
config = Config()


if __name__ == "__main__":
    # Test configuration
    print("=== Configuration Test ===")
    print(config)
    print(f"\nDatabase Connection String: {config.database.connection_string}")
    print(f"Kafka Topic: {config.kafka.topic}")
    print(f"Regions: {', '.join(config.producer.regions)}")