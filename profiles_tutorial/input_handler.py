import getpass
from enum import Enum
from typing import List, Dict
from config import PREDEFINED_ID_TYPES

class InputSteps(Enum):
    ACCOUNT = "Account (ex: ina31471.us-east-1): "
    USERNAME = "Username: "
    PASSWORD = "Password: "
    ROLE = "Role: "
    WAREHOUSE = "Warehouse: "
    INPUT_DATABASE = "Input Database name: "
    INPUT_SCHEMA = "Input Schema name: "
    OUTPUT_DATABASE = "Output Database name: "
    OUTPUT_SCHEMA = "Output Schema name: "

    @classmethod
    def common(cls):
        return [cls.ACCOUNT, cls.USERNAME, cls.PASSWORD, cls.ROLE, cls.WAREHOUSE]

    @classmethod
    def input(cls):
        return [cls.INPUT_DATABASE, cls.INPUT_SCHEMA]

    @classmethod
    def output(cls):
        return [cls.OUTPUT_DATABASE, cls.OUTPUT_SCHEMA]

class InputHandler: 
    def __init__(self, fast_mode: bool):
        self.fast_mode = fast_mode

    def get_user_input(self, prompt, options=None, password=False, default=None):
        full_prompt = f"{prompt} "
        if default:
            full_prompt += f"(default: {default}) "
        full_prompt += "\n> "
        
        while True:
            if password:
                user_input = getpass.getpass(full_prompt).strip()
            else:
                user_input = input(full_prompt).strip()
            
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

    def collect_user_inputs(self, steps: List[InputSteps]) -> Dict[InputSteps, str]:
        inputs = {}
        current_step = 0
        while current_step < len(steps):
            step = steps[current_step]
            user_input = self.get_user_input(step.value, password=step == InputSteps.PASSWORD)
            if user_input.lower() == 'back':
                if current_step > 0:
                    current_step -= 1
                else:
                    print("You are already at the first step.")
            else:
                inputs[step] = user_input
                current_step += 1
        return inputs

    def display_multiline_message(self, message: str):
        for line in message.split("\n"):
            print(line)
            if not self.fast_mode:
                input("")

    def guide_id_type_input(self, entity_name):
        about_id_types = {"anonymous_id": """
                          RudderStack creates a cookie for each user when they visit your site.
                           This cookie is used to identify the user across different sessions.
                           Every single event from event stream will have an anonymous_id.""", 
                          "email": """If a customer signs up using their email, this can be used to identify customers across devices, or even when they clear cookies.""",
                          "user_id": """
                          Most apps/websites define their own user_id for signed in users. 
                          These are the "identified" users, and typically most common id type for computing various metrics such as daily active users, monthly active users, etc.""",
                          "device_id": """
                          Often, we also identify users through their device ids. 
                          Example, for an IoT device company like Wyze, the events sent from their devices will have device ids and not an email or user_id. 
                          The ID stitcher helps connect these events to the user_id/email of the user as long as there's even a single event linking them (ex: a 'register your device' event)""", 
                         "shopify_store_id": """
                         When you have a payment service provider like Shopify, you may have different store ids - ex, one for website, one for android app etc.
                         You don't usually want to identify your users with a shopify_store_id. But for this demo, lets do it, just to see what happens.""",
                         "shopify_customer_id": """Shopify customer id is a unique identifier for a customer in Shopify, given by Shopify. It is used to identify a customer across different sessions."""}
        selected_id_types = []
        print("We'll go through some common id types one by one. As this is a demo, please type the exact name as suggested")
        for n, expected_id_type in enumerate(PREDEFINED_ID_TYPES):
            while True:
                if n == 0:
                    print("The first id type is anonymous_id.")
                else:
                    print(f"Now, the next id: {expected_id_type}.")
                self.display_multiline_message(about_id_types[expected_id_type])
                if self.fast_mode:
                    default = expected_id_type
                else:
                    default = None
                user_input = self.get_user_input(f"\nLet's add '{expected_id_type}' as an id type for {entity_name}: ", default=default)
                if user_input.lower() == expected_id_type.lower():
                    selected_id_types.append(expected_id_type)
                    break
                else:
                    print(f"Please enter the exact name '{expected_id_type}'. Let's try again.")
        print(f"\nGreat. So, the id types associated with entity {entity_name} are:")
        for id_type in selected_id_types:
            print(f"\t - {id_type}")
        return selected_id_types