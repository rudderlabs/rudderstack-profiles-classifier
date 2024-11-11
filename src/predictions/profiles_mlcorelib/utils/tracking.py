import os
import yaml
import uuid
from pathlib import Path
import rudderstack.analytics as rudder_analytics
from profiles_mlcorelib.utils.constants import PREFERENCES_PATH

# TODO: Modify them to a prod settings. These are currently from a testing env
WRITE_KEY = "2ohOAcLQ7PQaN2s7jNEPFc1elNW"
DATA_PLANE_URL = "https://rudderstacjpoy.dataplane.rudderstack.com"

rudder_analytics.write_key = WRITE_KEY
rudder_analytics.dataPlaneUrl = DATA_PLANE_URL


class Analytics:
    preferences_path = str(PREFERENCES_PATH)

    def __init__(self):
        self.properties = self._load_properties()
        self.opted_out = self.properties.get("telemetry_opted_out", False)

        # If opted out, don't do anything else
        if not self.opted_out:
            self._ensure_anonymous_id()

    def _load_properties(self) -> dict:
        if not os.path.exists(self.preferences_path):
            os.makedirs(os.path.dirname(self.preferences_path), exist_ok=True)
            return {}
        try:
            with open(self.preferences_path, "r") as f:
                return yaml.safe_load(f) or {}
        except yaml.YAMLError:
            return {}

    def _save_properties(self):
        # Read existing properties
        existing_properties = {}
        if os.path.exists(self.preferences_path):
            with open(self.preferences_path, "r") as f:
                existing_properties = yaml.safe_load(f) or {}

        # Merge our properties with existing ones
        existing_properties.update(self.properties)

        # Save merged properties
        os.makedirs(os.path.dirname(self.preferences_path), exist_ok=True)
        with open(self.preferences_path, "w") as f:
            yaml.dump(existing_properties, f)

    def _ensure_anonymous_id(self):
        if "anonymous_id" not in self.properties:
            self.properties["anonymous_id"] = str(uuid.uuid4())
            self._save_properties()

    def mark_consent_shown(self):
        self.properties["telemetry_consent_shown"] = True
        self._save_properties()

    def show_consent_message(self, logger):
        """Display consent message if it hasn't been shown before and user hasn't opted out."""
        consent_is_shown = self.properties.get("telemetry_consent_shown", False)
        if not consent_is_shown:
            consent_message = """\n\n
            Privacy Policy regarding Telemetry:
            - This tool collects usage statistics to help us improve the product.
            - We do not collect or store any sensitive information from your models.
            - You can opt out by adding 'telemetry_opted_out: true' to ~/.pb/properties.yaml
            """
            logger.info(consent_message)
            self.mark_consent_shown()

    def track(self, event_name: str, properties: dict = None):
        if not self.opted_out:
            if properties is None:
                properties = {}

            rudder_analytics.track(
                anonymous_id=self.properties["anonymous_id"],
                event=event_name,
                properties=properties,
            )
