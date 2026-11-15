# ONAIR.MK news feed

Static news feed for [onair.mk](https://onair.mk). A scheduled GitHub Actions job
fetches the public RSS feeds of Macedonian news portals and the public YouTube
RSS feeds of Macedonian channels every 15 minutes and publishes JSON files to the
`gh-pages` branch. The website reads those files directly: no database, no API
keys, no AI.

| File | Content |
| --- | --- |
| `latest.json` | 300 newest articles (homepage) |
| `news.json` | all articles of the last 4 days |
| `videos.json` | videos of the last 30 days |
| `channels.json` | YouTube channels with video counts |
| `status.json` | per-source health of the last run |

Feed URL: `https://neutser.github.io/onair-news-feed/latest.json`

## Change sources

Edit `config/sources.json` (news portals) or `config/channels.json` (YouTube
channel ids) and push to `main`; the job reruns automatically.

## Run locally

```bash
node scripts/build-feed.mjs
```

Only headlines, short snippets and links are stored. All content belongs to the
linked publishers.
