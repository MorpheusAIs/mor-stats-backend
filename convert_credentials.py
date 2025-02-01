import json
import os

def convert_credentials_to_env_string():
    # Read the credentials file
    with open('credentials.json', 'r') as file:
        credentials = json.load(file)
    
    # Convert to JSON string and escape any single quotes
    credentials_str = json.dumps(credentials).replace("'", "\\'")
    
    # Create the environment variable line
    new_line = f"GOOGLE_APPLICATION_CREDENTIALS='{credentials_str}'"
    
    # Read existing .env file or create empty string if it doesn't exist
    env_content = ""
    if os.path.exists('.env'):
        with open('.env', 'r') as file:
            lines = file.readlines()
            # Keep all lines that don't start with GOOGLE_APPLICATION_CREDENTIALS
            env_content = ''.join(line for line in lines 
                                if not line.startswith('GOOGLE_APPLICATION_CREDENTIALS='))
    
    # Append the new credentials line
    with open('.env', 'w') as file:
        file.write(env_content)
        # Add newline before credentials if there's existing content
        if env_content and not env_content.endswith('\n'):
            file.write('\n')
        file.write(new_line + '\n')
    
    print("Successfully updated GOOGLE_APPLICATION_CREDENTIALS in .env file!")
    print("Preview of the new credentials (first 100 characters):")
    print(new_line[:100] + "...")

if __name__ == "__main__":
    convert_credentials_to_env_string()