### Quick start

```bash
python -m scraper.main \
  --input schools.xlsx \
  --output contacts.xlsx \
  # add --no-continue if you want a brand‑new run


The script logs into LinkedIn once, caches cookies, then:

searches for each school,

filters People → Current company,

iterates over every profile on every page,

sends collected text to OpenAIIntegration.fetch_response(),

validates & stores the JSON,

writes/updates contacts.csv atomically so a crash never corrupts data.

Legal notice: Web‑scraping LinkedIn violates their Terms of Service.
This code is provided for educational purposes only; use at your own risk.


---

### That’s it!

You now have a modular, easily‑extensible scraper with:

* **robust selectors** isolated in one file,  
* **safe incremental writes** to avoid data loss,  
* **pluggable browser & headless mode**,  
* **prompt‑template centralisation**,  
* strict **JSON schema validation**, and  
* readiness for CI, Docker, or orchestration.

Feel free to adapt coding style, logging, or retry/back‑off logic to match your production conventions.