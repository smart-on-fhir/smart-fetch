---
title: Low Level Operations
parent: SMART Fetch
nav_order: 15
# audience: non-programmers vaguely familiar with the project
# type: how-to
---

# Direct Access to Low Level Export Operations

## Bulk Exports
You can do a direct bulk export into a folder by calling `smart-fetch bulk`.

See `--help` for all the options.

## Crawls
You can do a direct crawl into a folder by calling `smart-fetch crawl`.

See `--help` for all the options.

## Hydration Tasks
You can run hydration tasks on a folder of existing NDJSON by calling `smart-fetch hydrate`.

See `--help` for all the options.

## Single Resource Queries
You can ask for a single resource or do a small search by calling `smart-fetch single`.
This is especially helpful for debugging.

For example:
```shell
smart-fetch single Patient/abc
smart-fetch single Patient?active=true
```

See `--help` for all the options.
