import ray
from google import genai


@ray.remote
class GemW:

    def __init__(self):
        self.client = genai.Client()

    def ask(self, content):
        print("================== ASK GEM ===============")
        response = self.client.models.generate_content(
            model="gemini-2.5-flash",
            contents=content,
        )
        obj_ref = ray.put(response)
        return obj_ref