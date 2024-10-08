import getpass
from enum import Enum
from typing import List, Dict

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
        for line in message.strip().split("\n"):
            print(line)
            input("")

    def guide_id_type_input(self, entity_name):
        predefined_id_types = ['device_id', 'email', 'user_id', 'shopify_customer_id', 'anonymous_id', 'shopify_store_id']
        selected_id_types = []
        print("We'll go through some common id types one by one. As this is a demo, please type the exact name as suggested")
        for expected_id_type in predefined_id_types:
            while True:
                user_input = self.get_user_input(f"\nLet's add '{expected_id_type}' as an id type for {entity_name}: ")
                if user_input.lower() == expected_id_type.lower():
                    selected_id_types.append(expected_id_type)
                    break
                else:
                    print(f"Please enter the exact name '{expected_id_type}'. Let's try again.")
        print(f"\nGreat. So, the id types associated with entity {entity_name} are:")
        for id_type in selected_id_types:
            print(f"\t - {id_type}")
        return selected_id_types