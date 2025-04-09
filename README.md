# Google Sheets Combiner - Streamlit App

This Streamlit application allows you to combine data from multiple Google Sheets into a single CSV file. It tracks changes in the sheets and only downloads new or modified data.

## Features

- View and manage spreadsheet configurations
- Add new spreadsheets to track
- Combine data from multiple sheets
- Track changes and only download new/modified data
- Display combined data in an interactive table

## Setup Instructions

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up Google Sheets API credentials:
   - Create a Google Cloud Project
   - Enable the Google Sheets API
   - Create a service account and download the credentials JSON file
   - Share your Google Sheets with the service account email

## Local Development

Run the app locally:
```bash
streamlit run streamlit_app.py
```

## Deployment to Streamlit Cloud

1. Create a GitHub repository and push this code
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub account
4. Select this repository
5. Add your Google Sheets API credentials as a secret:
   - Key: `gcp_service_account`
   - Value: The contents of your service account JSON file

## Configuration

The app uses the following configuration files:
- `spreadsheets_config.json`: List of spreadsheets to track
- `sheets_tracking_app.json`: Tracks changes in the sheets

## License

MIT
