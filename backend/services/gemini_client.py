# gemini_client.py

import os
import google.genai as genai

class GeminiClient:
    """
    A client to interact with the Google Gemini API.
    """

    def __init__(self):
        """
        Initializes the Gemini client and configures it with an API key.
        """
        self.api_key = os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY environment variable must be set.")
        
        genai.configure(api_key=self.api_key)
        # Use the model from environment variable, or default to 'gemini-2.5-flash'
        model_name = os.getenv("GOOGLE_GEMINI_MODEL", 'gemini-2.5-flash')
        self.model = genai.GenerativeModel(model_name)
        
        print(f"GeminiClient initialized successfully with model: {model_name}")

    def get_ai_response(self, prompt: str) -> str:
        """
        Sends a prompt to the Gemini model and returns the text response.

        Args:
            prompt: The input prompt for the AI model.

        Returns:
            The generated text response from the model.
        """
        response = self.model.generate_content(prompt)
        return response.text