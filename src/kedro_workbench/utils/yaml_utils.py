import yaml
from datetime import datetime

def datetime_representer(dumper, data):
    return dumper.represent_scalar(u'!datetime', data.isoformat())

def pandas_timestamp_to_datetime(timestamp):
    return timestamp.to_pydatetime()

# Custom representer for Pandas Timestamp objects
def pandas_timestamp_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:timestamp', pandas_timestamp_to_datetime(data).isoformat())

def generate_html_v1(report_data):
    # Function to generate HTML for each section
    def generate_section_html(data, section_title, key_map):
        html = f'<h2 class="section-title">{section_title}</h2>'
        for item in data:
            html += '<div class="cve-item">'
            for key, label in key_map.items():
                if key in item:
                    html += f'<p><strong>{label}:</strong> {item[key]}</p>'
            html += '</div>'
        return html

    # Start of the HTML document
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{report_data['report_title']}</title>
        <style>
            body {{ font-family: 'Arial', sans-serif; background-color: white; color: #333; margin: 0; padding: 0; }}
            .container {{ max-width: 800px; margin: auto; padding: 20px; }}
            h1, h2, h3, h4, h5, h6 {{ color: #333; }}
            .report-title {{ text-align: center; }}
            .cve-item {{ background-color: #f5f5f5; border: 1px solid #ddd; padding: 10px; margin-bottom: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="report-title">{report_data['report_title']}</h1>
            <p class="report-description">{report_data['report_description']}</p>
    """

    # Generate HTML for each section
    html += generate_section_html(report_data['section_1_data'], "MSRC Posts", {"title": "Title", "source": "Source", "Summary": "Summary"})
    html += generate_section_html(report_data['section_2_data'], "Affected Products", {"product_name": "Product Name", "product_version": "Product Version", "relationship_count": "Relationship Count"})
    html += generate_section_html(report_data['section_3_data'], "Symptoms", {"symptom_description": "Symptom", "published_dates": "Published Date", "symptom_count": "Symptom Count"})
    html += generate_section_html(report_data['section_4_data'], "Fixes", {"fix_description": "Fix Description", "published_dates": "Published Date", "fix_count": "Fix Count"})

    # End of the HTML document
    html += """
        </div>
    </body>
    </html>
    """

    return html