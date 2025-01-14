from huggingface_hub import InferenceClient
import os

# Load your Hugging Face API token from the environment variable
hf_token = os.getenv("HF_TOKEN")
client = InferenceClient(token=hf_token)

# Model ID for Falcon-7B-Instruct
model_id = "tiiuae/falcon-7b-instruct"

def generate_response(prompt):
    """
    Generates a response using the Falcon-7B-Instruct model via the Hugging Face Inference API.
    
    Args:
        prompt (str): The input prompt/question to be processed by the model.
        
    Returns:
        str: The generated response from the model or an error message.
    """
    try:
        print("Generating response...")
        # Send the request to the Inference API
        response = client.text_generation(prompt, model=model_id, max_new_tokens=256)

        # Debugging: Print the raw response for inspection
        #print("\n[DEBUG] Raw API Response:", response)

        # Handle string responses
        if isinstance(response, str):
            return response

        # Handle list/dictionary responses
        if isinstance(response, list) and len(response) > 0:
            if "generated_text" in response[0]:
                return response[0]["generated_text"]
            else:
                return "No 'generated_text' found in response."

        return "Unexpected response format."

    except Exception as e:
        return f"An error occurred: {e}"

def main():
    """
    Main function to take user input and generate responses from the model.
    """
    print("Welcome to MetaboChat!")
    while True:
        prompt = input("\nEnter your prompt (or type 'exit' to quit): ")
        if prompt.lower() == "exit":
            print("Exiting MetaboChat. Goodbye!")
            break
        
        # Generate and display the response
        response = generate_response(prompt)
        print("\nResponse:", response)

if __name__ == "__main__":
    main()
