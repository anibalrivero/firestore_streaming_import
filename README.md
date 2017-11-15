= Firebase Streaming Import

- Utilizes ijson python json streaming library along with the official firestore library to import a large json piecemeal into Google Cloud Firestore (BETA).

== How to run:

=== Requirements: 
- `sudo apt install libyajl2`
- Create the virtual environment if you wish (recommended)
- run `pip install -r requirements.txt`
- Download a Firebase service file of your project

=== How to run

```bash 
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/keyfile.json"
```

```
usage: import.py [-h] collection json_file

Import a large json file into a Firestore via json Streaming. 

positional arguments:
  collection  Specify the Firestore base collection
  json_file   The JSON file to import.

optional arguments:
  -h, --help  show this help message and exit
```

