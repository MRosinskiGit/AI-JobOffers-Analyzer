# AI Job Offers Scraper

This project is for personal use, but I share the code if someone needs it. In a meantime I'm cleaning up the code and implementing more sites to track. It's more like proof of concept, but it works well with jenkins.

## Requirements
- Python 3.11+
- `.env` file with DeepSeek API key and your own prompts for candidate profile and expectations (prompts in the code are in Polish, but you can replace them with your own).

Example `.env` file:
```
DEEPSEEK_API_KEY=your_api_key
PROFILE_PROMPT="Your prompt for candidate profile"
EXPECTATIONS_PROMPT="Your prompt for expectations"
```

## How it works
1. The script scrapes job offers from Just Join IT (so far the only implemented) and other job offers sites.
2. Extracts data from the offers.
3. Sends them to the external DeepSeek API for match analysis.
4. Results are saved to a `.db` file handled by SQLite3.

## How to run
1. Install required libraries:
   ```bash
   pip install -r requirements.txt
   ```
2. Install Playwright:
   ```bash
   playwright install
   ```
3. Run the main async script:
   ```bash
   python main-async.py
   ```

## Note
The code works for now, but may break if the Just Join IT frontend changes.
