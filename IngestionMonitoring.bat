IF exist venv ( echo venv exists & cd venv\Scripts & activate & cd ..\.. & pip install -r requirements.txt & python main.py ) ELSE ( python -m venv .\venv && echo venv created & cd venv\Scripts & activate & cd ..\.. & pip install -r requirements.txt & python main.py)

