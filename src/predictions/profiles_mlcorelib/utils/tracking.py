import os
import yaml
import uuid
from pathlib import Path
import rudderstack.analytics as rudder_analytics
from profiles_mlcorelib.utils.constants import (
    PREFERENCES_PATH,
    TELEMETRY_OPT_OUT_KEY,
    TELEMETRY_CONSENT_SHOWN_KEY,
    WRITE_KEY,
    DATA_PLANE_URL,
)


class Analytics:
    def __init__(self):
        rudder_analytics.write_key = WRITE_KEY
        rudder_analytics.dataPlaneUrl = DATA_PLANE_URL
        self.preferences_path = PREFERENCES_PATH
        self.properties = self._load_properties()
        self.opted_out = self.properties.get(TELEMETRY_OPT_OUT_KEY, False)

        # If opted out, don't do anything else
        if not self.opted_out:
            self._ensure_anonymous_id()

    def _load_properties(self) -> dict:
        self.preferences_path.parent.mkdir(parents=True, exist_ok=True)
        self.preferences_path.touch()
        try:
            return yaml.safe_load(self.preferences_path.read_text()) or {}
        except Exception:
            return {}

    def _save_properties(self):
        try:
            existing_properties = (
                yaml.safe_load(self.preferences_path.read_text()) or {}
            )
            existing_properties.update(self.properties)
            self.preferences_path.write_text(yaml.dump(existing_properties))
        except Exception as e:
            print(f"Error saving preferences: {e}")
            pass

    def _ensure_anonymous_id(self):
        if "anonymous_id" not in self.properties:
            self.properties["anonymous_id"] = str(uuid.uuid4())
            self._save_properties()

    def mark_consent_shown(self):
        self.properties[TELEMETRY_CONSENT_SHOWN_KEY] = True
        self._save_properties()

    def show_consent_message(self, logger):
        """Display consent message if it hasn't been shown before and user hasn't opted out."""
        consent_is_shown = self.properties.get(TELEMETRY_CONSENT_SHOWN_KEY, False)
        if not consent_is_shown:
            consent_message = f"""\n\n
            Privacy Policy regarding Telemetry:
            - This tool collects usage statistics to help us improve the product.
            - We do not collect or store any sensitive information from your models.
            - You can opt out by adding '{TELEMETRY_OPT_OUT_KEY}: true' to {str(self.preferences_path)}
            """
            logger.info(consent_message)
            self.mark_consent_shown()

    def track(self, event_name: str, properties: dict = None):
        try:
            if not self.opted_out:
                if properties is None:
                    properties = {}

                rudder_analytics.track(
                    anonymous_id=self.properties["anonymous_id"],
                    event=event_name,
                    properties=properties,
                )
        except Exception:
            # Failed to track event - should not have any impact on the user
            pass
