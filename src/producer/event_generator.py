"""
Realistic Telecom Event Generator
Generates and sends telecom events to Kafka with realistic patterns
"""

import json
import time
import uuid
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TelecomEventGenerator:
    """Generates realistic telecom events"""
    
    # Event type probabilities (realistic distribution)
    EVENT_WEIGHTS = {
        'data_session': 0.50,      # 50% - most common
        'call': 0.25,              # 25%
        'sms': 0.15,               # 15%
        'balance_recharge': 0.07,  # 7%
        'service_activation': 0.03 # 3%
    }
    
    # Call subtypes
    CALL_SUBTYPES = ['incoming', 'outgoing']
    
    # Regions in Kazakhstan
    REGIONS = [
        'Almaty', 'Astana', 'Shymkent', 'Aktobe',
        'Karaganda', 'Taraz', 'Pavlodar', 'Oskemen',
        'Semey', 'Atyrau', 'Oral', 'Kostanay'
    ]
    
    # Service types for activation
    SERVICES = [
        'Unlimited_Internet', 'International_Calls',
        'Roaming_Package', 'Music_Streaming', 'Video_HD'
    ]
    
    def __init__(self, kafka_bootstrap_servers: str = 'localhost:9092'):
        """Initialize the event generator"""
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = None
        self.event_count = 0
        self.error_count = 0
        
        # Track used phone numbers for realistic patterns
        self.active_numbers = set()
        
    def connect_kafka(self) -> bool:
        """Connect to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"✅ Connected to Kafka at {self.kafka_bootstrap_servers}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def disconnect_kafka(self):
        """Disconnect from Kafka"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")
    
    def generate_msisdn(self) -> str:
        """Generate realistic Kazakhstan phone number (masked)"""
        # Kazakhstan numbers start with 7, followed by area code
        # Format: 7701234**** (last 4 digits masked)
        prefix = random.choice(['770', '771', '775', '776', '777', '778'])
        number = f"{prefix}{random.randint(1000000, 9999999):07d}"
        # Mask last 4 digits
        masked = number[:7] + "****"
        
        # Add to active numbers occasionally
        if random.random() < 0.3:  # 30% chance
            self.active_numbers.add(masked)
            
        # Sometimes reuse numbers (realistic behavior)
        if self.active_numbers and random.random() < 0.4:  # 40% chance
            return random.choice(list(self.active_numbers))
            
        return masked
    
    def get_current_hour_weight(self) -> float:
        """
        Get traffic weight based on current hour (peak hours simulation)
        Peak hours: 8-10 AM, 12-2 PM, 6-10 PM
        """
        hour = datetime.now().hour
        
        # Peak hours have higher weights
        if 8 <= hour <= 10:  # Morning peak
            return 1.5
        elif 12 <= hour <= 14:  # Lunch peak
            return 1.3
        elif 18 <= hour <= 22:  # Evening peak
            return 2.0
        elif 0 <= hour <= 6:  # Night (low traffic)
            return 0.3
        else:  # Normal hours
            return 1.0
    
    def generate_call_event(self) -> Dict[str, Any]:
        """Generate a call event"""
        subtype = random.choice(self.CALL_SUBTYPES)
        
        # Realistic call durations (in seconds)
        # Most calls are short, some are longer
        if random.random() < 0.7:  # 70% short calls (< 5 min)
            duration = random.randint(30, 300)
        elif random.random() < 0.9:  # 20% medium calls (5-15 min)
            duration = random.randint(300, 900)
        else:  # 10% long calls (15-60 min)
            duration = random.randint(900, 3600)
        
        return {
            'event_type': 'call',
            'event_subtype': subtype,
            'duration_seconds': duration,
            'data_mb': None,
            'amount': None
        }
    
    def generate_sms_event(self) -> Dict[str, Any]:
        """Generate an SMS event"""
        subtype = random.choice(self.CALL_SUBTYPES)
        
        return {
            'event_type': 'sms',
            'event_subtype': subtype,
            'duration_seconds': None,
            'data_mb': None,
            'amount': None
        }
    
    def generate_data_session_event(self) -> Dict[str, Any]:
        """Generate a data session event"""
        # Realistic data usage patterns
        if random.random() < 0.5:  # 50% light usage (< 100 MB)
            data_mb = random.uniform(1, 100)
            duration = random.randint(60, 1800)  # 1-30 min
        elif random.random() < 0.8:  # 30% medium usage (100-500 MB)
            data_mb = random.uniform(100, 500)
            duration = random.randint(1800, 7200)  # 30min-2hr
        else:  # 20% heavy usage (> 500 MB)
            data_mb = random.uniform(500, 2000)
            duration = random.randint(3600, 14400)  # 1-4 hours
        
        return {
            'event_type': 'data_session',
            'event_subtype': None,
            'duration_seconds': duration,
            'data_mb': round(data_mb, 2),
            'amount': None
        }
    
    def generate_balance_recharge_event(self) -> Dict[str, Any]:
        """Generate a balance recharge event"""
        # Common recharge amounts in Kazakhstan (Tenge)
        amounts = [500, 1000, 2000, 3000, 5000, 10000]
        amount = random.choice(amounts)
        
        return {
            'event_type': 'balance_recharge',
            'event_subtype': None,
            'duration_seconds': None,
            'data_mb': None,
            'amount': float(amount)
        }
    
    def generate_service_activation_event(self) -> Dict[str, Any]:
        """Generate a service activation event"""
        service = random.choice(self.SERVICES)
        
        return {
            'event_type': 'service_activation',
            'event_subtype': service,
            'duration_seconds': None,
            'data_mb': None,
            'amount': None
        }
    
    def generate_event(self) -> Dict[str, Any]:
        """Generate a random telecom event"""
        # Select event type based on weights
        event_type = random.choices(
            list(self.EVENT_WEIGHTS.keys()),
            weights=list(self.EVENT_WEIGHTS.values())
        )[0]
        
        # Generate event based on type
        if event_type == 'call':
            event_data = self.generate_call_event()
        elif event_type == 'sms':
            event_data = self.generate_sms_event()
        elif event_type == 'data_session':
            event_data = self.generate_data_session_event()
        elif event_type == 'balance_recharge':
            event_data = self.generate_balance_recharge_event()
        else:  # service_activation
            event_data = self.generate_service_activation_event()
        
        # Add common fields
        event = {
            'event_id': str(uuid.uuid4()),
            'msisdn': self.generate_msisdn(),
            'region': random.choice(self.REGIONS),
            'cell_tower_id': random.randint(1000, 9999),
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            **event_data
        }
        
        return event
    
    def send_event(self, topic: str, event: Dict[str, Any]) -> bool:
        """Send event to Kafka"""
        try:
            # Use msisdn as key for partitioning
            key = event['msisdn']
            future = self.producer.send(topic, key=key, value=event)
            
            # Wait for send to complete (with timeout)
            future.get(timeout=10)
            
            self.event_count += 1
            return True
            
        except KafkaError as e:
            self.error_count += 1
            logger.error(f"Failed to send event: {e}")
            return False
    
    def run(self, topic: str = 'telecom_events', duration_seconds: Optional[int] = None,
            events_per_second: int = 10):
        """
        Run the event generator
        
        Args:
            topic: Kafka topic to send events to
            duration_seconds: How long to run (None = infinite)
            events_per_second: Target events per second
        """
        if not self.connect_kafka():
            logger.error("Cannot start generator without Kafka connection")
            return
        
        logger.info(f"🚀 Starting event generator...")
        logger.info(f"   Topic: {topic}")
        logger.info(f"   Rate: {events_per_second} events/second")
        logger.info(f"   Duration: {'Infinite' if duration_seconds is None else f'{duration_seconds}s'}")
        
        start_time = time.time()
        delay_between_events = 1.0 / events_per_second
        
        try:
            while True:
                # Check duration
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break
                
                # Apply peak hour weighting
                hour_weight = self.get_current_hour_weight()
                current_rate = int(events_per_second * hour_weight)
                current_delay = 1.0 / max(current_rate, 1)
                
                # Generate and send event
                event = self.generate_event()
                self.send_event(topic, event)
                
                # Log progress every 100 events
                if self.event_count % 100 == 0:
                    logger.info(
                        f"📊 Sent {self.event_count} events "
                        f"(Errors: {self.error_count}, "
                        f"Rate: ~{current_rate}/s)"
                    )
                
                # Sleep to maintain rate
                time.sleep(current_delay)
                
        except KeyboardInterrupt:
            logger.info("\n⚠️  Generator stopped by user")
        except Exception as e:
            logger.error(f"❌ Generator error: {e}")
        finally:
            self.disconnect_kafka()
            elapsed = time.time() - start_time
            logger.info(f"\n📈 Final Stats:")
            logger.info(f"   Total Events: {self.event_count}")
            logger.info(f"   Errors: {self.error_count}")
            logger.info(f"   Duration: {elapsed:.2f}s")
            logger.info(f"   Avg Rate: {self.event_count/elapsed:.2f} events/s")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Telecom Event Generator')
    parser.add_argument(
        '--kafka',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--topic',
        default='telecom_events',
        help='Kafka topic (default: telecom_events)'
    )
    parser.add_argument(
        '--rate',
        type=int,
        default=10,
        help='Events per second (default: 10)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Run duration in seconds (default: infinite)'
    )
    
    args = parser.parse_args()
    
    # Create and run generator
    generator = TelecomEventGenerator(kafka_bootstrap_servers=args.kafka)
    generator.run(
        topic=args.topic,
        events_per_second=args.rate,
        duration_seconds=args.duration
    )


if __name__ == '__main__':
    main()