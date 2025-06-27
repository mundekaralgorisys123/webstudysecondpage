# Web Scraper

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#file-structure)
- [Dependencies](#dependencies)
- [Configuration](#configuration)
- [Logging](#logging)
- [Error Handling](#error-handling)
- [Future Improvements](#future-improvements)

## Introduction

This project is a web scraper designed to extract product data from jewelry e-commerce websites, specifically targeting Tiffany & Co. products. The scraper fetches details such as product names, prices, metal types, diamond weights, and images, storing the extracted information in an Excel file for further analysis.

## Features

- **Dynamic Web Scraping :** Uses Playwright to handle JavaScript-rendered content.

- **Product Data Extraction :** Extracts product names, prices, metal types, and diamond weights.

- **Image Handling :** Downloads and embeds product images into an Excel file.

- **Formatted Excel Output :** Saves data in a structured and visually appealing format.

- **Automatic Date and Time Logging :** Adds timestamps for each scraped entry.

- **Error Handling :** Ensures robustness in case of missing elements or network issues.


## Installation

### Prerequisites

**Ensure you have the following installed:**

- Python 3.10+

- Playwright

- OpenPyXL

- Requests

- Steps

1. **Clone the repository :**

```bash
git clone <repo-url>
cd <project-directory>
```

2. **Install dependencies :**
```bash
pip install -r requirements.txt
```

3. **Install Playwright browsers :**
```bash
playwright install
```

## Usage

**Run the scraper with :**
```bash

python scraper.py
```
The extracted data will be saved as an Excel file in **ScapData/static/ExcelData/Products.xlsx**

## File Structure
```bash
project-directory/
│-- scraper.py              # Main script for web scraping
│-- requirements.txt        # Dependencies list
│-- ScapData/
│   └── static/
│       └── ExcelData/      # Folder to store extracted Excel files
```

## Dependencies

- **Playwright :** For web automation and scraping dynamic pages.

- **BeautifulSoup :** For parsing HTML content.

- **Requests :** For downloading images.

- **OpenPyXL :** For handling Excel file operations.

## Configuration

Modify the following variables in **scraper.py** to customize behavior:

- **EXCEL_DATA_PATH :** Directory for saving the Excel file.

- **headless=False :** Change to True for headless scraping.

## Logging

Print statements are used for debugging and tracking execution.

You can add logging using Python’s logging module for better monitoring.

## Error Handling

- **Missing Elements :** Uses checks to prevent crashes if selectors are incorrect.

- **Image Download Failures :** Catches exceptions and continues execution.

- **Timeout Handling :** Ensures smooth execution if pages take time to load.

## Future Improvements

- Support for multiple websites with different layouts.

- Implement asynchronous image downloads for better performance.

- Store data in a database instead of an Excel file for scalability.

- Implement proper logging instead of print statements.

<hr>

This project provides an efficient and scalable solution for extracting jewelry product data dynamically. Contributions and improvements are welcome!