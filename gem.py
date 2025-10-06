import os

import ray
from google import genai

import dotenv

from cluster_nodes.cluster_utils.base import BaseActor

dotenv.load_dotenv()

@ray.remote
class GemW(BaseActor):

    def __init__(self, model="gemini-2.5-flash"):
        BaseActor.__init__(self)
        self.model = model
        self.client = genai.Client(
            api_key=os.environ.get("GEMINI_API_KEY")
        )
        print("GEMW INITIALIZED")


    def ask(self, content):
        print("================== ASK GEM ===============")
        response = self.client.models.generate_content(
            model=self.model,
            contents=content,
        )
        text = response.text
        obj_ref = ray.put(text)
        return obj_ref

