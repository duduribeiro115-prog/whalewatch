# Deploy WhaleWatch to the cloud (Render) — get a public link

Goal: a permanent URL like `https://whalewatch-xxxx.onrender.com` that anyone
can open, running 24/7 without your laptop.

You do this **once**. No coding, no git command line — all through the browser.

---

## Before you deploy: populate the database locally

So the public site launches already full of data, build + load it on your Mac first.

```
cd ~/whalewatch
python3 whalewatch.py build       # fund directory (you've already done this)
python3 whalewatch.py preload     # loads ~150 big funds -> fills Stocks + Alerts
```

`preload` takes a few minutes (it's downloading from SEC). When it finishes,
`whalewatch.db` in this folder contains everything. That file ships with the deploy.

---

## Step 1 — Put the files on GitHub (browser only)

1. Make a free account at https://github.com (skip if you have one).
2. Click the **+** (top-right) → **New repository**.
3. Name it `whalewatch`, leave it **Public**, click **Create repository**.
4. On the new repo page, click **"uploading an existing file"**.
5. Drag in these files from your `~/whalewatch` folder:
   - `whalewatch.py`
   - `requirements.txt`
   - `render.yaml`
   - `.gitignore`
   - `whalewatch.db`   ← the populated database (this is what carries your data)
6. Click **Commit changes**.

> Tip: in Finder press `Cmd+Shift+.` to show hidden files if you can't see `.gitignore`.

---

## Step 2 — Deploy on Render

1. Make a free account at https://render.com (sign in **with GitHub** — easiest).
2. Click **New +** → **Web Service**.
3. Choose **Build and deploy from a Git repository** → connect your `whalewatch` repo.
4. Render reads `render.yaml` and fills in the settings automatically. If asked, confirm:
   - **Runtime:** Python
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `gunicorn whalewatch:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120`
   - **Instance type:** Free
5. Click **Create Web Service**. First build takes ~2–4 minutes.
6. When it says **Live**, your link is at the top, e.g. `https://whalewatch-xxxx.onrender.com`.
   Share that. Open it on your phone and "Add to Home Screen" for an app feel.

---

## Two things to know about the free tier

- **It sleeps after ~15 min of no visitors** and takes ~30 seconds to wake on the
  next visit. Normal for free hosting. A paid instance ($7/mo) stays awake.
- **Disk resets on redeploy.** Funds people open on the live site won't be saved
  permanently. To refresh the public data, re-run `preload` locally, re-upload
  `whalewatch.db` to GitHub, and Render auto-redeploys.

## When you want this to be "real" (next step, not now)

- Add a Render **persistent disk** + point `WW_DB` at it so data survives restarts.
- Add a Render **Cron Job** running `python3 whalewatch.py refresh` daily to pull
  new filings automatically (the daily-update feature).
- Add a real **CUSIP→ticker** mapping so stocks link to prices/logos.

Ask me and I'll set any of these up.
