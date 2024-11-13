from .config import PREDEFINED_ID_TYPES

from profiles_rudderstack.reader import Reader


class IOHandler:
    def __init__(self, reader: Reader, fast_mode: bool):
        self.reader = reader
        self.fast_mode = fast_mode

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
            if line:
                print(line)
            if not self.fast_mode:
                self.reader.get_input("")

    def guide_id_type_input(self, entity_name):
        about_id_types = {
            "anon_id": """
                          RudderStack creates a cookie for each user when they visit your site.
                           This cookie is used to identify the user across different sessions anonymously, prior to the users identifying themselves.
                           Every single event from event stream will have an anonymous_id.""",
            "email": """If a customer signs up using their email, this can be used to identify customers across devices, or even when they clear cookies.""",
            "user_id": """
                          Most apps/websites define their own user_id for signed in users. 
                          These are the "identified" users, and typically the most common id type for computing various metrics such as daily active users, monthly active users, etc.""",
            "device_id": """
                          This is a more specialized id type specific to Secure Solutions, LLC. Since they sell IoT devices, they want to link specific device ids back to specific users.
                          The ID stitcher helps connect these events to the user_id/email of the user as long as there's even a single event linking them (ex: a 'register your device' event).""",
            "shopify_customer_id": """
                         An Ecommerce platform like Shopify will auto generate a unique identifier for each unique user who completes any sort of checkout conversion and thereby become an official customer. 
                         This unique identifier is called, “shopify_customer_id”. We will want to bring this ID Type in in order to enhance the user ID Graph. """,
            "shopify_store_id": """
                         When you have a payment service provider like Shopify, you may have different stores or apps - one for a website, one for an android app etc. 
                         These different apps or stores may have their own unique identifiers. It is important to note here, this id type is of a higher level grain than the user identifiers. 
                         One store id may be linked to many users.  And so for the user entity, we should not bring it in. 
                         But for the purposes of this tutorial, we will go ahead and bring it in to show you what happens and how to troubleshoot when a resolved profile id has merged multiple users together.""",
        }
        selected_id_types = []
        print(
            "We'll go through some common id types one by one. As this is a demo, please type the exact name as suggested"
        )
        for n, expected_id_type in enumerate(PREDEFINED_ID_TYPES):
            while True:
                if n == 0:
                    print(f"The first id type is {expected_id_type}.")
                else:
                    print(f"Now, the next id: {expected_id_type}.")
                self.display_multiline_message(about_id_types[expected_id_type])
                default = expected_id_type
                user_input = self.get_user_input(
                    f"\nLet's add '{expected_id_type}' as an id type for {entity_name}: ",
                    default=default,
                    options=[default],
                )
                if user_input.lower() == expected_id_type.lower():
                    selected_id_types.append(expected_id_type)
                    break
                else:
                    print(
                        f"Please enter the exact name '{expected_id_type}'. Let's try again."
                    )
        print(f"\nGreat. So, the id types associated with entity {entity_name} are:")
        for id_type in selected_id_types:
            print(f"\t - {id_type}")
        return selected_id_types
