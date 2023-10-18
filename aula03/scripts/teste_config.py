import os
from dotenv import load_dotenv
load_dotenv()

print(os.environ.get('films_raw_path'))
print(type(os.environ.get('films_raw_path')))
