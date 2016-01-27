# Bus Time Content

Provides content for Bus Time stops and routes database.

## Validating

Content validation is performed via built-in validator script.

Python 3.3—3.5 and PIP are required to be installed.

First, dependencies should be resolved:

```
$ pip install -r validator/requirements.txt
```

Then, just run the validator providing it with the path to 
the content:

```
$ python validator/validator.py -d content/
```