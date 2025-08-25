# JSON Event Analyzer - Flask Web Application

A modern web interface for analyzing JSON event data based on filter parameters. This Flask application provides a user-friendly way to upload JSON files and analyze events using the same logic as the original command-line script.

## Features

- 🎯 **Web-based Interface**: Modern, responsive UI built with Bootstrap 5
- 📁 **File Upload**: Drag & drop or click to upload JSON files
- 📊 **Visual Analytics**: Charts and progress bars showing event distribution
- 🔍 **Flexible Filtering**: Analyze events based on custom filter parameters and root IDs
- 📋 **Detailed Results**: View matching and non-matching event IDs
- 🖨️ **Print Support**: Print-friendly results page
- 🔌 **API Endpoint**: RESTful API for programmatic access
- 🛡️ **Security**: File validation and secure upload handling

## Installation

1. **Clone or download the project files**

2. **Install Python dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Flask application**:

   ```bash
   python app.py
   ```

4. **Open your browser** and navigate to:
   ```
   http://localhost:5000
   ```

## Usage

### Web Interface

1. **Upload JSON File**: Drag and drop your JSON file or click to browse
2. **Set Parameters**:
   - **Filter Parameter**: The parameter to filter by (e.g., `fbc`)
   - **Root ID**: The root ID to match against (e.g., `305546688`)
3. **Options**: Check "Show event IDs" to display detailed event ID lists
4. **Analyze**: Click "Analyze Events" to process your data
5. **View Results**: See statistics, charts, and detailed event information

### API Usage

The application also provides a RESTful API endpoint:

```bash
POST /api/analyze
Content-Type: application/json

{
    "json_data": {
        "event1": {
            "output__305546688__querystring__fbc": "value1"
        },
        "event2": {
            "output__305546688__querystring__fbc": null
        }
    },
    "filter_param": "fbc",
    "root_id": "305546688",
    "show_ids": true
}
```

Response:

```json
{
  "total_events": 2,
  "target_events": 1,
  "success_rate": 50.0,
  "target_event_ids": ["event1"],
  "failed_event_ids": ["event2"],
  "output_content": "total events: 2\ntarget events: 1\n..."
}
```

## File Structure

```
zapier-history-hacker/
├── app.py                 # Main Flask application
├── parse.py              # Original command-line script
├── requirements.txt      # Python dependencies
├── README.md            # This file
├── templates/           # HTML templates
│   ├── base.html       # Base template with styling
│   ├── index.html      # Upload form page
│   └── results.html    # Results display page
└── uploads/            # Temporary upload directory (auto-created)
```

## How It Works

The application analyzes JSON data by:

1. **Loading JSON**: Parses the uploaded JSON file
2. **Event Processing**: Iterates through each event in the data
3. **Filter Matching**: Looks for the pattern `output__{root_id}__querystring__{filter_param}`
4. **Classification**: Categorizes events as matching or non-matching
5. **Statistics**: Calculates totals, success rates, and generates visualizations

## Example JSON Format

Your JSON file should contain events with the following structure:

```json
{
  "event_001": {
    "output__305546688__querystring__fbc": "some_value",
    "other_field": "other_value"
  },
  "event_002": {
    "output__305546688__querystring__fbc": null,
    "other_field": "other_value"
  }
}
```

## Configuration

### Environment Variables

- `FLASK_ENV`: Set to `development` for debug mode
- `SECRET_KEY`: Change the secret key in production

### File Upload Limits

- Maximum file size: 16MB
- Allowed extensions: `.json` only
- Files are automatically cleaned up after processing

## Development

### Running in Development Mode

```bash
export FLASK_ENV=development
python app.py
```

### Running in Production

For production deployment, consider using:

- **WSGI Server**: Gunicorn or uWSGI
- **Reverse Proxy**: Nginx or Apache
- **Process Manager**: Supervisor or systemd

Example with Gunicorn:

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

## Security Considerations

- Change the `SECRET_KEY` in production
- Implement proper file size limits
- Add authentication if needed
- Use HTTPS in production
- Validate all user inputs

## Troubleshooting

### Common Issues

1. **File Upload Fails**: Ensure the file is a valid JSON and under 16MB
2. **No Results**: Check that your filter parameter and root ID match the data structure
3. **Server Won't Start**: Verify all dependencies are installed with `pip install -r requirements.txt`

### Error Messages

- **"Invalid JSON"**: Your file contains malformed JSON
- **"File not found"**: The uploaded file couldn't be processed
- **"Missing parameters"**: Fill in all required fields

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve the application.

## License

This project is open source and available under the MIT License.
