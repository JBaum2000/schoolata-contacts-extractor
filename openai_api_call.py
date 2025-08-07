import requests
import traceback
import time
import os
from dotenv import load_dotenv

load_dotenv()

class OpenAIIntegration:
    
    openai_api_key = os.getenv('OPENAI_API_KEY')
    language_models = ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo", "gpt-4o", "gpt-4o-mini", "o1-preview", "o1-mini"]

    def __init__(self):
        self.headers = {
            "Authorization": f"Bearer {OpenAIIntegration.openai_api_key}",
            # "OpenAI-Organization": "org-BmYxrwj0ESNtpXN2YP0nszD9", # Personal Organization ID
            "OpenAI-Organization": "org-hXrhIEuhzmUaTYtJbHkRKl4W", # Venture Organization ID
            "Content-Type": "application/json"
        }

    def fetch(self, data):
        MAX_RETRIES = 3
        REQUEST_TIMEOUT = 420
        retries = 0
        sleep_time = 1
        while retries < MAX_RETRIES:
            try:
                response = requests.post("https://api.openai.com/v1/chat/completions", headers=self.headers, json=data, timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    content = response.json()
                    print('prompt_tokens', content['usage']['prompt_tokens'])
                    return content["choices"][0]["message"]["content"]
                else:
                    raise Exception(f"HTTP Error: {response.status_code}, message: {response.content}")
    
            except Exception as e:
                traceback.print_exc()
                print(f"Error: {str(e)}")
                time.sleep(sleep_time)
                sleep_time *= 2
                retries += 1
                if retries == MAX_RETRIES:
                    answer = input(f"Max retries reached. Would you like to continue? (y/n): ")
                    if answer.lower() != "y":
                        raise e

    def fetch_response(self, string: str, image_path: str = None, model: str = "gpt-4o") -> str:
        if model in OpenAIIntegration.language_models:
            data = {
                "model": model,
                "messages": [{"role": "user", "content": string}]
            }
        return self.fetch(data)