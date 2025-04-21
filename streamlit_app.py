import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configuration
OUTPUT_CSV_BASE_NAME = 'combined_requests'
TRACKING_DATA_FILE = 'sheets_tracking_app.json'
TRACK_CHANGES = True
FORCE_REFRESH = False
CONFIG_DIR = 'user_configs'

def ensure_user_config_dir():
    """Ensure the user configuration directory exists"""
    if not os.path.exists(CONFIG_DIR):
        os.makedirs(CONFIG_DIR)

def get_user_config_path(username):
    """Get the path to a user's configuration file"""
    return os.path.join(CONFIG_DIR, f'{username}_config.json')

def load_user_config(username):
    """Load configuration for a specific user"""
    config_path = get_user_config_path(username)
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
    except Exception as e:
        st.error(f"Error loading user configuration: {e}")
    return {'spreadsheets': []}

def save_user_config(username, config):
    """Save configuration for a specific user"""
    config_path = get_user_config_path(username)
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        st.error(f"Error saving user configuration: {e}")
        return False

def initialize_session_state():
    """Initialize session state variables"""
    if 'username' not in st.session_state:
        st.session_state.username = None
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'new_spreadsheet_id' not in st.session_state:
        st.session_state.new_spreadsheet_id = ""
    if 'new_sheet_name' not in st.session_state:
        st.session_state.new_sheet_name = ""
    if 'show_success' not in st.session_state:
        st.session_state.show_success = False
    if 'success_message' not in st.session_state:
        st.session_state.success_message = ""
    if 'show_error' not in st.session_state:
        st.session_state.show_error = False
    if 'error_message' not in st.session_state:
        st.session_state.error_message = ""

def handle_login():
    """Handle login form submission"""
    username = st.session_state.username_input
    password = st.session_state.password_input
    if username and password:
        st.session_state.username = username
        st.session_state.authenticated = True
    else:
        st.session_state.error_message = "Please enter both username and password"
        st.session_state.show_error = True

def handle_logout():
    """Handle logout button click"""
    st.session_state.username = None
    st.session_state.authenticated = False
    st.session_state.new_spreadsheet_id = ""
    st.session_state.new_sheet_name = ""

def handle_add_spreadsheet():
    """Handle adding a new spreadsheet"""
    if st.session_state.new_spreadsheet_id and st.session_state.new_sheet_name:
        spreadsheets_config = load_user_config(st.session_state.username).get('spreadsheets', [])
        spreadsheets_config.append([st.session_state.new_spreadsheet_id, st.session_state.new_sheet_name])
        if save_user_config(st.session_state.username, {'spreadsheets': spreadsheets_config}):
            st.session_state.new_spreadsheet_id = ""
            st.session_state.new_sheet_name = ""
            st.session_state.show_success = True
            st.session_state.success_message = "Spreadsheet added successfully!"
        else:
            st.session_state.show_error = True
            st.session_state.error_message = "Failed to save spreadsheet configuration"
    else:
        st.session_state.show_error = True
        st.session_state.error_message = "Please provide both Spreadsheet ID and Sheet Name"

def handle_delete_spreadsheet(index):
    """Handle deleting a spreadsheet"""
    spreadsheets_config = load_user_config(st.session_state.username).get('spreadsheets', [])
    spreadsheets_config.pop(index)
    if save_user_config(st.session_state.username, {'spreadsheets': spreadsheets_config}):
        st.session_state.show_success = True
        st.session_state.success_message = "Spreadsheet removed successfully!"
    else:
        st.session_state.show_error = True
        st.session_state.error_message = "Failed to remove spreadsheet"

def login_page():
    """Display login page"""
    st.title("📊 Google Sheets Combiner")
    
    with st.form("login_form"):
        st.text_input("Username", key="username_input")
        st.text_input("Password", type="password", key="password_input")
        submit = st.form_submit_button("Login", on_click=handle_login)

def get_output_csv_path():
    """Generate output CSV path with timestamp to ensure uniqueness"""
    current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return f"{OUTPUT_CSV_BASE_NAME}_{current_datetime}.csv"

def setup_google_sheets_api():
    """Setup the Google Sheets API client"""
    try:
        # For Streamlit Cloud, we'll use the credentials from secrets
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/spreadsheets'  # Add write permission
            ]
        )
        service = build('sheets', 'v4', credentials=creds)
        return service
    except Exception as e:
        st.error(f"Failed to setup Google Sheets API: {e}")
        return None

def load_tracking_data():
    """Load tracking data from previous runs"""
    if os.path.exists(TRACKING_DATA_FILE):
        try:
            with open(TRACKING_DATA_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Error loading tracking data: {e}")
    return {
        'last_run': None,
        'sheets_data': {}
    }

def save_tracking_data(tracking_data):
    """Save tracking data for future runs"""
    try:
        with open(TRACKING_DATA_FILE, 'w') as f:
            json.dump(tracking_data, f, indent=2)
        st.success(f"Saved tracking data to {TRACKING_DATA_FILE}")
    except Exception as e:
        st.error(f"Error saving tracking data: {e}")

def calculate_content_hash(data_frame):
    """Calculate a hash for the entire DataFrame to detect any changes"""
    if data_frame is None or len(data_frame) == 0:
        return hash("empty")
    return hash(pd.util.hash_pandas_object(data_frame).sum())

def get_sheet_metadata(service, spreadsheet_id, sheet_name):
    """Get metadata about the sheet to detect changes"""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in spreadsheet.get('sheets', []):
            if sheet.get('properties', {}).get('title') == sheet_name:
                grid_props = sheet.get('properties', {}).get('gridProperties', {})
                return {
                    'row_count': grid_props.get('rowCount', 0),
                    'column_count': grid_props.get('columnCount', 0),
                    'modified_time': spreadsheet.get('properties', {}).get('modifiedTime', '')
                }
        return None
    except Exception as e:
        st.error(f"Error getting sheet metadata: {e}")
        return None

def download_sheet_data(service, spreadsheet_id, sheet_name, tracking_data):
    """Download data from a specific Google Sheet tab"""
    try:
        if FORCE_REFRESH:
            pass  # Removed info message
        else:
            metadata = get_sheet_metadata(service, spreadsheet_id, sheet_name)
            if metadata is None:
                return None
            
            sheet_key = f"{spreadsheet_id}_{sheet_name}"
            previous_metadata = tracking_data['sheets_data'].get(sheet_key, {}).get('metadata', {})
            previous_content_hash = tracking_data['sheets_data'].get(sheet_key, {}).get('content_hash', 0)
            
            row_count_changed = metadata['row_count'] != previous_metadata.get('row_count', 0)
            modified_time_changed = metadata['modified_time'] != previous_metadata.get('modified_time', '')
            
            if TRACK_CHANGES and not row_count_changed and not modified_time_changed:
                pass  # Removed info message
        
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_name
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            return None
        
        headers = values[0]
        data_rows = []
        for row in values[1:]:
            if len(row) < len(headers):
                row = row + [None] * (len(headers) - len(row))
            elif len(row) > len(headers):
                row = row[:len(headers)]
            data_rows.append(row)
        
        df = pd.DataFrame(data_rows, columns=headers)
        df['source_spreadsheet'] = spreadsheet_id
        df['source_sheet'] = sheet_name
        
        if len(headers) > 1:
            column_a_name = headers[0]
            column_b_name = headers[1]
            
            filtered_df = df[(df[column_a_name] == "New Request") & 
                        (df[column_b_name].notna()) & 
                        (df[column_b_name] != "")]
                    
            filtered_row_count = len(filtered_df)
            
            if filtered_row_count == 0:
                return None
            
            df = filtered_df
        else:
            return None
        
        if FORCE_REFRESH or not TRACK_CHANGES:
            sheet_key = f"{spreadsheet_id}_{sheet_name}"
            metadata = get_sheet_metadata(service, spreadsheet_id, sheet_name) if FORCE_REFRESH else metadata
            
            tracking_data['sheets_data'][sheet_key] = {
                'metadata': metadata,
                'content_hash': calculate_content_hash(df),
                'last_processed': datetime.now().isoformat()
            }
            
            return df
        
        current_content_hash = calculate_content_hash(df)
        sheet_key = f"{spreadsheet_id}_{sheet_name}"
        
        if current_content_hash == previous_content_hash:
            tracking_data['sheets_data'][sheet_key]['metadata'] = metadata
            tracking_data['sheets_data'][sheet_key]['last_checked'] = datetime.now().isoformat()
            
            return None
        
        tracking_data['sheets_data'][sheet_key] = {
            'metadata': metadata,
            'content_hash': current_content_hash,
            'last_processed': datetime.now().isoformat()
        }
        
        return df
    
    except HttpError as error:
        st.error(f"Error accessing spreadsheet: {error}")
        return None
    except Exception as e:
        st.error(f"Error processing spreadsheet: {e}")
        return None

def update_sheet_status(service, spreadsheet_id, sheet_name, df):
    """Update the status of processed rows in the source sheet"""
    try:
        # Get the full sheet data to find row indices
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_name
        ).execute()
        
        values = result.get('values', [])
        if not values:
            return
        
        headers = values[0]
        
        # Create a batch update request
        batch_update_request = {
            'valueInputOption': 'USER_ENTERED',  # Changed from RAW to USER_ENTERED
            'data': []
        }
        
        # For each row in the original sheet
        for i, sheet_row in enumerate(values[1:], start=1):  # Skip header row
            if len(sheet_row) > 1:  # Ensure row has enough columns
                # Check if this row has "New Request" in column A
                if sheet_row[0] == "New Request":
                    # Check if this row's content matches any row in our filtered DataFrame
                    for _, df_row in df.iterrows():
                        # Match based on content in column B
                        if sheet_row[1] == df_row[headers[1]]:
                            # Add to batch update
                            batch_update_request['data'].append({
                                'range': f'{sheet_name}!A{i+1}',  # +1 for 1-based indexing
                                'values': [['Submitted / In Progress']]
                            })
                            break  # Found a match, move to next sheet row
        
        # Only make the API call if we have updates to make
        if batch_update_request['data']:
            try:
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=batch_update_request
                ).execute()
                st.success(f"Updated {len(batch_update_request['data'])} rows in {sheet_name}")
            except Exception as e:
                st.error(f"Failed to update status in sheet {sheet_name}: {str(e)}")
            
    except Exception as e:
        st.error(f"Error updating sheet status: {str(e)}")

def combine_and_save_data(spreadsheets_config):
    """Download data from multiple spreadsheets and combine into one CSV"""
    output_csv_path = get_output_csv_path()
    tracking_data = load_tracking_data()
    tracking_data['last_run'] = datetime.now().isoformat()
    
    service = setup_google_sheets_api()
    if not service:
        return False
    
    all_dfs = []
    sheet_data = []  # Store sheet info for status updates
    
    for spreadsheet_info in spreadsheets_config:
        spreadsheet_id, sheet_name = spreadsheet_info
        
        df = download_sheet_data(service, spreadsheet_id, sheet_name, tracking_data)
        if df is not None and not df.empty:
            all_dfs.append(df)
            sheet_data.append({
                'spreadsheet_id': spreadsheet_id,
                'sheet_name': sheet_name,
                'df': df
            })
    
    if not all_dfs:
        st.warning("No new data was found to combine.")
        save_tracking_data(tracking_data)
        return False
    
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Save to CSV
    combined_df.to_csv(output_csv_path, index=False)
    save_tracking_data(tracking_data)
    
    # Update status in source sheets
    with st.spinner("Updating source sheets..."):
        for sheet_info in sheet_data:
            update_sheet_status(
                service,
                sheet_info['spreadsheet_id'],
                sheet_info['sheet_name'],
                sheet_info['df']
            )
    
    return combined_df

def main():
    st.set_page_config(
        page_title="Sheets Combiner",
        page_icon="📊",
        layout="wide"
    )
    
    # Initialize session state
    initialize_session_state()
    
    # Ensure user config directory exists
    ensure_user_config_dir()
    
    # Check if user is authenticated
    if not st.session_state.authenticated:
        login_page()
        if st.session_state.show_error:
            st.error(st.session_state.error_message)
            st.session_state.show_error = False
        return
    
    st.title("📊 Google Sheets Combiner")
    
    # Add logout button
    if st.sidebar.button("Logout", on_click=handle_logout):
        return
    
    # Display current user
    st.sidebar.write(f"Logged in as: {st.session_state.username}")
    
    # Show success/error messages if any
    if st.session_state.show_success:
        st.success(st.session_state.success_message)
        st.session_state.show_success = False
    if st.session_state.show_error:
        st.error(st.session_state.error_message)
        st.session_state.show_error = False
    
    # Load user-specific configuration
    user_config = load_user_config(st.session_state.username)
    spreadsheets_config = user_config.get('spreadsheets', [])
    
    # Combine data button in a centered column
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Combine Data", type="primary", use_container_width=True):
            if spreadsheets_config:
                with st.spinner("Combining data from spreadsheets... This may take a moment."):
                    result_df = combine_and_save_data(spreadsheets_config)
                    if result_df is not False:
                        st.success("✅ Data combined successfully!")
                        st.dataframe(result_df)
            else:
                st.error("No spreadsheets configured. Please add at least one spreadsheet.")
    
    st.markdown("---")  # Add a separator
    
    # Add new spreadsheet
    st.header("Add New Spreadsheet")
    st.text_input("New Spreadsheet ID", key="new_spreadsheet_id")
    st.text_input("New Sheet Name", key="new_sheet_name")
    
    if st.button("Add Spreadsheet", on_click=handle_add_spreadsheet):
        pass
    
    st.markdown("---")  # Add a separator
    
    # Display current configuration
    st.header("Current Spreadsheets Configuration")
    if spreadsheets_config:
        for i, (spreadsheet_id, sheet_name) in enumerate(spreadsheets_config):
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                st.text_input(f"Spreadsheet ID {i+1}", spreadsheet_id, key=f"spreadsheet_id_{i}")
            with col2:
                st.text_input(f"Sheet Name {i+1}", sheet_name, key=f"sheet_name_{i}")
            with col3:
                if st.button("🗑️", key=f"delete_{i}", on_click=handle_delete_spreadsheet, args=(i,)):
                    pass
    else:
        st.warning("No spreadsheets configured")

if __name__ == "__main__":
    main() 