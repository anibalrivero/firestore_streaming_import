# Firestore Streaming Import

Utilizes ijson python json streaming library along with the official firestore library to import a large json piecemeal into Google Cloud Firestore (BETA). This does a specific kind of import and expects a specific type of json to import.

## JSON expected format

```json
{
  {
    "document id":{
      "key": "value",
      "object_name": {
        "key": "value",
        ...
      }
    }
  },
  ....
}
```

## Requirements

- `sudo apt install libyajl2`
- Create the virtual environment if you wish (recommended)
- `pip install -r requirements.txt`
- Download a Firebase service file of your project

## How to run

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

