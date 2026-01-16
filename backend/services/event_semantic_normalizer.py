# event_semantic_normalizer.py

from typing import List, Dict, Any

class EventSemanticNormalizer:
    """
    Cleans and normalizes raw event data from analytics platforms.

    This component standardizes event names and property keys based on
    pre-defined mapping rules to ensure data consistency.
    """

    def __init__(self, event_name_map: Dict[str, str], property_key_map: Dict[str, str]):
        """
        Initializes the normalizer with mapping rules.

        Args:
            event_name_map: A dictionary to map raw event names to standardized names.
                           (e.g., {"level_start": "level_started"})
            property_key_map: A dictionary to map raw property keys to standardized keys.
                              (e.g., {"user_id": "player_id"})
        """
        self.event_name_map = event_name_map
        self.property_key_map = property_key_map

    def _normalize_event_name(self, event_name: str) -> str:
        """Normalizes a single event name using the mapping."""
        return self.event_name_map.get(event_name, event_name)

    def _normalize_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Normalizes the keys within an event's properties dictionary."""
        if not isinstance(properties, dict):
            return properties  # Return as-is if not a dictionary

        normalized_props = {}
        for key, value in properties.items():
            new_key = self.property_key_map.get(key, key)
            normalized_props[new_key] = value
        return normalized_props

    def normalize_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processes a list of raw event dictionaries to normalize them.

        Args:
            events: A list of raw event dictionaries from Amplitude.

        Returns:
            A list of cleaned and normalized event dictionaries.
        """
        if not events:
            return []

        normalized_events = []
        for event in events:
            # Create a copy to avoid modifying the original list of dicts
            new_event = event.copy()
            new_event['event_type'] = self._normalize_event_name(event['event_type'])
            new_event['event_properties'] = self._normalize_properties(event.get('event_properties', {}))
            new_event['user_properties'] = self._normalize_properties(event.get('user_properties', {}))
            
            normalized_events.append(new_event)
            
        print(f"Normalized {len(normalized_events)} events.")
        return normalized_events