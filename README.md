### Setup

Working on Python 3.7.11

1. Create python virtual environment

```
virtualenv -p python3.7 venv
```

2. Activate virtual environment

```
source venv/bin/activate

# this should now point at the venv python
which python

```

3. Install dependencies

```
python -m pip install -r requirements.txt
```

4. Start the Jupyter Notebook server

```
python -m juypyter notebook
```

5. You should now be able to open and run Jupter notebooks (`VCFTest.ipynb`) via `http://localhost:8888`

