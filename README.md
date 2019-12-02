

```bash
cd appengine
pipenv lock -r > requirements.txt && gcloud app deploy --project=jxpress-playground
GOOGLE_CLOUD_PROJECT=jxpress-playground GOOGLE_APPLICATION_CREDENTIALS=<?> pipenv run python main.py
```

```bash
gcloud tasks queues create node-updater --project=jxpress-playground
```

# Check

Regenerate API_KEY when all demo are done.