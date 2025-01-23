from profiles_rudderstack.reader import Reader

from .config import PREDEFINED_ID_TYPES


class IOHandler:
    def __init__(self, reader: Reader, fast_mode: bool):
        self.reader = reader
        self.fast_mode = fast_mode

    def display_message(self, message: str):
        print(message)

    def get_user_input(self, prompt, options=None, password=False, default=None):
        full_prompt = f"{prompt} "
        if default:
            full_prompt += f"(default: {default}) "
        full_prompt += "\n> "

        while True:
            user_input = self.reader.get_input(full_prompt, password).strip()

            if not user_input:
                if default:
                    return default
                else:
                    print("Please try again")
                    continue

            if options:
                if user_input.lower() in [opt.lower() for opt in options]:
                    return user_input
                print("Invalid input. Please try again.")
            else:
                return user_input

    def display_multiline_message(self, message: str):
        for line in message.split("\n"):
            if not self.fast_mode and line:
                self.reader.get_input("")
            if line:
                print(line)
