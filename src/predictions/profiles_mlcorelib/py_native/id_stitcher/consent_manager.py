import json
from pathlib import Path
from typing import Union
from profiles_rudderstack.logger import Logger

# TODO: Uncomment the following line after adding the Reader class to the profiles_rudderstack package
# from profiles_rudderstack.reader import Reader


class ConsentManager:
    LLM_CONSENT_KEY = "llm_consent"
    PREFERENCES_FILE = "preferences.json"

    def __init__(self, logger: Logger):
        config_dir = Path.home() / ".pb"
        if not config_dir.exists():
            config_dir.mkdir(parents=True, exist_ok=True)
        self.consent_file = config_dir / self.PREFERENCES_FILE
        self.logger = logger

    @property
    def consent_message(self) -> str:
        msg = f"""
        This process includes LLM-based analysis which may send some of your text data to LLM providers Anthropic and OpenAI as part of the API calls. 
        Would you like to proceed? This choice will be rememberd for future runs. (you can reset this choice by deleting the consent file at {self.consent_file})
        This is an optional step and you can choose to not consent to LLM usage and continue with the analysis excluding the LLM-based analysis.
        Do you consent to LLM usage? (yes/no): 
        """
        return msg

    def get_stored_consent(self) -> Union[bool, None]:
        try:
            if not self.consent_file.exists():
                return None
            with open(self.consent_file, "r") as f:
                data = json.load(f)
                return data.get(self.LLM_CONSENT_KEY, None)
        except json.JSONDecodeError:
            # Delete existing file if it is corrupted
            self.consent_file.unlink(missing_ok=True)
            return None
        except Exception as e:
            # For other exceptions, return None - assume prior consent doesn't exist
            self.logger.warn(
                f"Error reading consent file {self.consent_file}: {e}. Asking for new consent."
            )
            return None

    def save_consent(self, consent: bool) -> None:
        self.consent_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.consent_file, "w") as f:
            json.dump({self.LLM_CONSENT_KEY: consent}, f)

    def prompt_for_consent(self, reader) -> bool:
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
