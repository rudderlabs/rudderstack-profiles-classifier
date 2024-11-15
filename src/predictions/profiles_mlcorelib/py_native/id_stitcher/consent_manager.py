import yaml
from pathlib import Path
from typing import Union
from profiles_rudderstack.logger import Logger
from profiles_mlcorelib.utils.constants import (
    PREFERENCES_PATH,
    CONFIG_DIR,
    LLM_CONSENT_KEY,
)

from profiles_rudderstack.reader import Reader


class ConsentManager:
    def __init__(self, logger: Logger):
        if not CONFIG_DIR.exists():
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        self.preferences_file = PREFERENCES_PATH
        self.logger = logger

    @property
    def consent_message(self) -> str:
        msg = f"""
        This process includes LLM-based analysis which may send some of your text data to LLM providers Anthropic and OpenAI as part of the API calls. 
        Would you like to proceed? This choice will be rememberd for future runs. (you can reset this choice by deleting the consent file at {self.preferences_file})
        This is an optional step and you can choose to not consent to LLM usage and continue with the analysis excluding the LLM-based analysis.
        Do you consent to LLM usage? (yes/no): 
        """
        return msg

    def get_stored_consent(self) -> Union[bool, None]:
        try:
            if not self.preferences_file.exists():
                return None
            with open(self.preferences_file, "r") as f:
                data = yaml.safe_load(f) or {}
                return data.get(LLM_CONSENT_KEY, None)
        except yaml.YAMLError:
            self.logger.warn(
                f"Error reading preferences file {self.preferences_file}: YAML is corrupted. Creating new file."
            )
            # Don't delete, create new empty file
            with open(self.preferences_file, "w") as f:
                yaml.dump({}, f)
            return None
        except Exception as e:
            # For other exceptions, return None - assume prior consent doesn't exist
            self.logger.warn(
                f"Error reading consent file {self.preferences_file}: {e}. Asking for new consent."
            )
            return None

    def save_consent(self, consent: bool) -> None:
        existing_preferences = {}
        if self.preferences_file.exists():
            with open(self.preferences_file, "r") as f:
                existing_preferences = yaml.safe_load(f) or {}

        existing_preferences[LLM_CONSENT_KEY] = consent
        self.preferences_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.preferences_file, "w") as f:
            yaml.dump(existing_preferences, f)

    def prompt_for_consent(self, reader: Reader) -> bool:
        retry_count = 0
        while retry_count < 3:
            response = reader.get_input(self.consent_message).lower().strip()
            if response == "yes":
                consent = True
            elif response == "no":
                consent = False
            else:
                retry_count += 1
                print(f"Invalid response: {response}. Please enter 'yes' or 'no'.\n")
                continue
            self.save_consent(consent)
            return consent
        print("Max retries reached. Skipping LLM-based analysis for this run.")
        return False
