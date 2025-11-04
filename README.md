# 📊 Zambia Regulatory Compliance Survey

A comprehensive Streamlit web application for collecting, analyzing, and managing regulatory compliance data from formal businesses in Zambia. This tool enables researchers to conduct structured interviews and generate insights to inform policy reforms.

![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-07405E?style=for-the-badge&logo=sqlite&logoColor=white)

## 🚀 Quick Start

### Installation & Setup

```bash
# 1. Clone or download the project files
# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the application
streamlit run app.py

# 4. Access via browser
# http://localhost:8501
```

### Default Login Credentials
- **Admin**: `admin` / `compliance2024`
- **Researcher**: `researcher` / `data2024`

> ⚠️ **Security Note**: Change credentials for production use!

## 📋 Features Overview

### 🎯 Core Functionality
- **Multi-section Interview Forms** (Sections A-D)
- **Business Profile Capture** with ISIC classification
- **Enhanced Procedure Tracking** with multiple entry modes
- **Compliance Burden Analysis**
- **Reform Priority Suggestions**

### ⚡ Enhanced Procedure Entry Modes
| Mode | Description | Use Case |
|------|-------------|----------|
| **⚡ Quick Manual** | Fast entry for individual procedures | Quick data entry |
| **🔧 Single Detailed** | Comprehensive data capture | In-depth analysis |
| **📊 Bulk Templates** | Multi-license template system | Multiple procedures |

### 📈 Analytics & Reporting
- **Real-time Compliance Dashboards**
- **Cost & Time Analysis**
- **Risk Scoring Algorithms**
- **Interactive Visualizations** (Plotly)
- **Multi-format Data Export** (CSV, Excel, JSON)

## 🛠️ Technical Setup

### Requirements

**Python**: 3.8 or higher

**Dependencies** (`requirements.txt`):
```txt
streamlit>=1.28.0
pandas>=2.0.0
plotly>=5.15.0
streamlit-tags>=1.2.0
openpyxl>=3.0.0
xlsxwriter>=3.1.0
python-dateutil>=2.8.0
```

### Installation Commands

```bash
# Basic installation
pip install -r requirements.txt

# Individual packages
pip install streamlit pandas plotly streamlit-tags openpyxl xlsxwriter python-dateutil

# Development setup (optional)
pip install pytest black flake8 python-dotenv
```

### Project Structure
```
zambia-compliance-survey/
├── app.py                 # Main application
├── requirements.txt       # Python dependencies
├── compliance_survey.db   # SQLite database (auto-created)
└── data/                 # Optional data directory
    ├── Complete_ISIC5_Structure_16Dec2022final.xlsx
    └── ISIC_Codes.xlsx
```

## 📖 User Guide

### For Interviewers

#### Interview Workflow
1. **Section A**: Business Profile & Classification
2. **Section B**: Regulatory Procedures & Licensing
3. **Section C**: Ongoing Compliance Analysis
4. **Section D**: Reform Priorities & Recommendations
5. **Dashboard**: Review & Analytics

#### Procedure Entry Modes

**⚡ Quick Manual Mode**
- Fastest way to add individual procedures
- Minimal required fields: Name, Authority, Cost, Days
- Perfect for rapid data entry

**🔧 Single Detailed Mode**
- Comprehensive data capture
- All fields: Time analysis, costs, complexity, documents
- Ideal for in-depth compliance analysis

**📊 Bulk Templates Mode**
- Add multiple licenses simultaneously
- Pre-configured sector templates
- Bulk configuration options

### Smart Templates Available

#### 🌾 Agribusiness Templates
- PACRA Business Registration
- ZRA Tax Registration
- Local Trading License
- ZEMA Environmental License
- Food and Drugs Act License
- WARMA Water Permit

#### 🏗️ Construction Templates
- NCC Registration
- Building Permit
- ZEMA EIA Permit
- Road Cutting Permit
- Planning Permission
- Occupational Certificate

### For Administrators

#### Admin Dashboard Features
- **📊 Database Statistics**: Interview counts and metrics
- **🔍 Search & Filter**: Find interviews by sector, district, status
- **💾 Data Export**: CSV, Excel, JSON formats
- **🛠️ System Tools**: Database maintenance and logs

#### Data Management
```python
# Export capabilities include:
- Full dataset (CSV/Excel/JSON)
- Filtered results
- Procedure-specific data
- Summary statistics
```

## 💾 Data Management

### Supported Export Formats
| Format | Features | Use Case |
|--------|----------|----------|
| **CSV** | Simple, compatible | Spreadsheet analysis |
| **Excel** | Multi-sheet, formatted | Reports & presentations |
| **JSON** | Structured data | API integration |

### Database Operations
- **Auto-backup**: Regular database backups recommended
- **Data Validation**: Built-in validation rules
- **Risk Scoring**: Automated compliance risk calculations
- **Audit Logging**: All admin actions recorded

## 🎯 Key Analytics Features

### Real-time Metrics
- Total compliance cost calculations
- Time burden analysis
- Risk scoring algorithms
- Sector-wise comparisons

### Visualization Types
- Interactive pie charts and bar graphs
- Cost breakdown analysis
- Time distribution charts
- Risk matrix visualization
- Application mode distribution

## 🔧 Troubleshooting

### Common Issues & Solutions

#### Database Connection Issues
```bash
# Reset database (deletes all data)
rm compliance_survey.db
# Restart application - database auto-creates
```

#### Package Installation Problems
```bash
# Upgrade pip first
python -m pip install --upgrade pip

# Install with specific index
pip install -r requirements.txt -i https://pypi.org/simple/

# Install packages individually if needed
pip install streamlit pandas plotly
```

#### ISIC Data Loading Issues
- Ensure Excel files are in project directory
- Check file names match expected patterns
- Verify openpyxl is installed correctly

### Performance Optimization
- Use bulk mode for multiple procedures
- Save drafts regularly during long interviews
- Export data periodically for backup
- Clear browser cache if UI becomes slow

## 🌐 Deployment

### Local Development
```bash
streamlit run app.py
```

### Production Considerations
- Use proper web server (nginx + gunicorn)
- Implement database encryption
- Set up SSL certificates
- Configure proper user authentication
- Schedule regular database backups

## 📊 Compliance Standards

### Data Standards
- **ISIC Codes**: International Standard Industrial Classification
- **Currency**: Zambian Kwacha (ZMW)
- **Time Units**: Calendar days
- **Sector Classification**: Agribusiness/Construction

### Regulatory Framework Alignment
- Zambia Development Agency (ZDA)
- Patents and Companies Registration Agency (PACRA)
- Zambia Revenue Authority (ZRA)
- Zambia Environmental Management Agency (ZEMA)
- Local Government Authorities

## 🔮 Future Enhancements

### Planned Features
- [ ] Multi-language support
- [ ] Advanced reporting engine
- [ ] API integration with government systems
- [ ] Mobile-responsive design
- [ ] Offline data collection
- [ ] Role-based access control

### Scalability Roadmap
- Database migration to PostgreSQL
- Cloud deployment options
- Load balancing for multiple users
- Automated backup systems

## 🆘 Support & Maintenance

### System Requirements
- **Python**: 3.8 or higher
- **RAM**: 4GB minimum, 8GB recommended
- **Storage**: 500MB free space
- **Browser**: Chrome, Firefox, or Safari (latest versions)

### Getting Help
1. Check the troubleshooting section above
2. Review application logs in admin panel
3. Ensure all dependencies are correctly installed
4. Verify file permissions and paths

### Maintenance Tips
- Regular database backups
- Monitor system logs
- Update dependencies periodically
- Validate data imports before processing

## 📄 License & Usage

### Usage Rights
- Internal use for regulatory compliance research
- Data ownership: Respect business confidentiality
- Code modifications: Document changes in admin logs

### Third-party Licenses
- Streamlit: Apache 2.0 License
- Plotly: MIT License
- Pandas: BSD 3-Clause License

---

**Developed for Zambia Regulatory Compliance Research**  

*For technical support or questions, contact the development team.*